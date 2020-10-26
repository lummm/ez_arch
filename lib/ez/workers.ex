defmodule Ez.Workers do
  @moduledoc """
  State looks like this:
  {
    addresses: %{addr -> service_name}
    jobs: %{addr -> job count}
    services: %{service_name -> MapSet of addresses}
    timeouts: %{addr -> timeout unix milliseconds}
  }
  """

  use GenServer
  require Logger

  @worker <<1>>
  @heartbeat <<1>>
  @reply <<2>>
  @ack <<3>>

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def worker_msg(msg) do
    GenServer.cast(__MODULE__, {:worker_msg, msg})
  end

  def req(req_id, return_addr, service_name, body) do
    # can req_id be optional?
    GenServer.cast(__MODULE__,
      {:req, req_id, return_addr, service_name, body})
  end

  def worker_engaged(addr) do
    GenServer.cast(__MODULE__, {:worker_engaged, addr})
  end

  def worker_unengaged(addr) do
    GenServer.cast(__MODULE__, {:worker_unengaged, addr})
  end

  def delete_workers(worker_addresses) do
    GenServer.cast(__MODULE__, {:delete, worker_addresses})
  end

  def get_timeouts do
    GenServer.call(__MODULE__, {:get, :timeouts})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    spawn_link(fn -> check_worker_loop() end)
    {:ok, %{
        addresses: %{},
        jobs: %{},
        services: %{},
        timeouts: %{},
     }}
  end

  @impl true
  def handle_cast({:worker_msg, msg}, state) do
    state = case msg do
              [addr, "", @worker, @heartbeat, sname] ->
                handle_heartbeat(state, addr, sname)
              [addr, "", @worker, @reply | rest] ->
                handle_reply(state, addr, rest)
              [addr, "", @worker, @ack, req_id] ->
                handle_ack(state, addr, req_id)
              _ ->
                Logger.warn("didn't recognize #{Kernel.inspect(msg)}")
                state
            end
    {:noreply, state}
  end
  def handle_cast({:req, req_id, return_addr, sname, body}, state) do
    spawn(fn ->
      do_request(
        state, req_id, return_addr, sname, body,
        Ez.Env.min_req_timeout()
      )
    end)
    {:noreply, state}
  end
  def handle_cast({:worker_engaged, addr}, state) do
    current_jobs = Map.get(state.jobs, addr, 0)
    state = put_in(state, [:jobs, addr], current_jobs + 1)
    {:noreply, state}
  end
  def handle_cast({:worker_unengaged, addr}, state) do
    current_jobs = Map.get(state.jobs, addr, 0)
    state = put_in(state, [:jobs, addr], current_jobs - 1)
    {:noreply, state}
  end
  def handle_cast({:delete, addresses}, state) do
    Enum.each(addresses, &(Logger.info(
              "deleting #{state.addresses[&1]} worker - #{Kernel.inspect(&1)}"
            )))
    new_services = addresses
    |> Enum.map(fn addr -> {addr, state.addresses[addr]} end)
    |> Enum.reduce(state.services, fn {addr, sname}, services ->
      new_workers = MapSet.delete(services[sname], addr)
      if 0 < MapSet.size(new_workers) do
        put_in(services, [sname], new_workers)
      else
        Map.delete(services, sname)
      end
    end)
    {:noreply, %{state |
                 addresses: Map.drop(state.addresses, addresses),
                 jobs: Map.drop(state.jobs, addresses),
                 services: new_services,
                 timeouts: Map.drop(state.timeouts, addresses),
                }}
  end

  @impl true
  def handle_call({:get, :timeouts}, _from, state) do
    {:reply, state.timeouts, state}
  end

  # private
  defp handle_heartbeat(state, addr, sname) do
    if not Map.has_key?(state.addresses, addr) do
      Logger.info("new worker for #{sname} - #{Kernel.inspect(addr)}")
    end
    workers = if Map.has_key?(state.services, sname) do
      state.services[sname]
    else
      MapSet.new()
    end
    state
    |> put_in([:services, sname], MapSet.put(workers, addr))
    |> put_in([:addresses, addr], sname)
    |> handle_common(addr)
  end

  defp handle_reply(state, addr, reply_frames) do
    spawn(fn ->
      worker_unengaged(addr)
      Ez.ZmqReq.reply(reply_frames)
    end)
    handle_common(state, addr)
  end

  defp handle_ack(state, addr, req_id) do
    spawn(fn ->
      info = Ez.Requests.pop_req(req_id)
      if info do
        send(info.pid, :ack)
      else
        Logger.error("no info found for req id #{req_id}")
      end
    end)
    handle_common(state, addr)
  end

  defp handle_common(state, addr) do
    state
    |> put_in([:timeouts, addr],
    :os.system_time(:millisecond) + Ez.Env.worker_lifetime())
  end

  defp select_worker(state, sname) do
    state.services[sname]
    |> MapSet.to_list()
    |> Stream.map(fn addr -> {addr, Map.get(state.jobs, addr, 0)} end)
    |> Enum.min_by(fn {_addr, jobs} -> jobs end)
  end

  defp do_request(
    state, req_id, return_addr, sname, body,
    retry_timeout
  ) do
    if not Map.has_key?(state.services, sname) do
      Logger.warn("no such service #{sname}")
      if retry_timeout && (retry_timeout < Ez.Env.max_req_timeout()) do
        Process.sleep(retry_timeout)
        do_request(state, req_id, return_addr, sname, body,
          retry_timeout * 2)
      else
        Ez.ZmqReq.reply(return_addr, req_id,
          ["ERR", "\"no such service\""])
      end
    else
      {addr, jobs} = select_worker(state, sname)
      if Ez.Env.backpressure_threshold() < jobs do
        Process.sleep(jobs * Ez.Env.backpressure_ratio())
      end
      Ez.Requests.put_req(req_id, self())
      Ez.WorkerListen.send_to_worker(
        addr, req_id, return_addr, body)
      receive do
        :ack -> worker_engaged(addr)
      after
        retry_timeout ->
          Logger.info("req timeout for #{sname}")
          if retry_timeout < Ez.Env.max_req_timeout() do
            do_request(state, req_id, return_addr, sname, body,
              retry_timeout * 2)
          else
              Ez.ZmqReq.reply(return_addr, req_id,
                ["ERR", "\"timeout\""])
          end
      end

    end
  end

  defp check_worker_loop do
    Process.sleep(Ez.Env.worker_lifetime())
    workers = get_timeouts()
    |> get_dead_workers()
    if length(workers) > 0 do
      delete_workers(workers)
    end
    check_worker_loop()
  end

  defp get_dead_workers(timeouts) do
    now = :os.system_time(:millisecond)
    timeouts
    |> Stream.filter(fn {_addr, timeout} -> timeout < now end)
    |> Enum.map(fn {addr, _timeout} -> addr end)
  end

end
