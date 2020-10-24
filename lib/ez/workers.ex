defmodule Ez.Workers do
  @moduledoc """
  State looks like this:
  {
    jobs: %{addr -> job count}
    services: %{service_name -> MapSet of addresses }
    timeouts: %{addr -> timeout unix milliseconds }
  }
  """

  use GenServer
  require Logger

  @worker <<1>>
  @heartbeat <<1>>
  @reply <<2>>

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
              _ ->
                Logger.warn("didn't recognize #{Kernel.inspect(msg)}")
                state
            end
    {:noreply, state}
  end
  def handle_cast({:req, req_id, return_addr, sname, body}, state) do
    spawn(fn ->
      do_request(state, req_id, return_addr, sname, body)
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
    Logger.info(
      "deleting workers - [#{Enum.join(addresses, "\n")}]")
    # I'd like it if I could look up the service name for an address
    to_delete = MapSet.new(addresses)
    jobs = Map.drop(state.jobs, addresses)
    services = state.jobs
    |> Enum.map(fn {sname, workers} ->
      {sname, MapSet.difference(workers, to_delete)}
    end)
    |> Enum.filter(fn {_sname, workers} ->
      MapSet.size(workers) < 1
    end)
    |> Map.new()
    timeouts = Map.drop(state.timeouts, addresses)
    {:noreply, %{
        jobs: jobs,
        services: services,
        timeouts: timeouts,
     }}
  end

  @impl true
  def handle_call({:get, :timeouts}, _from, state) do
    {:reply, state.timeouts, state}
  end

  # private
  defp handle_heartbeat(state, addr, sname) do
    workers = if Map.has_key?(state.services, sname) do
      state.services[sname]
    else
      Logger.info("new worker for #{sname} - '#{Kernel.inspect(addr)}'")
      MapSet.new()
    end
    state
    |> put_in([:services, sname], MapSet.put(workers, addr))
    |> handle_common(addr)
  end

  defp handle_reply(state, addr, reply_frames) do
    spawn(fn ->
      worker_unengaged(addr)
      Ez.ZmqReq.reply(reply_frames)
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

  defp do_request(state, req_id, return_addr, sname, body,
  retry \\ true) do
    if not Map.has_key?(state.services, sname) do
      Logger.warn("no such service #{sname}")
      if retry do
        Process.sleep(Integer.floor_div(Ez.Env.worker_lifetime(), 2))
        do_request(state, req_id, return_addr, sname, body, false)
      else
        Ez.ZmqReq.reply([
          return_addr, "", req_id,  "ERR", "\"no such service\""
        ])
      end
    else
      {addr, jobs} = select_worker(state, sname)
      if Ez.Env.backpressure_threshold() < jobs do
        Process.sleep(jobs * Ez.Env.backpressure_ratio())
      end
      worker_engaged(addr)
      Ez.WorkerListen.send_to_worker(addr, req_id, return_addr, body)
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
