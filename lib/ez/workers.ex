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

  defstruct addresses: %{}, jobs: %{}, services: %{}, timeouts: %{}

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
  def get_state do
    GenServer.call(__MODULE__, {:get, :state})
  end


  # Server callbacks
  @impl true
  def init(_args) do
    spawn_link(fn -> check_worker_loop() end)
    {:ok, %Ez.Workers{}}
  end

  @impl true
  def handle_cast({:worker_msg, msg}, state) do
    state = case msg do
              [addr, "", @worker, @heartbeat, sname] ->
                handle_heartbeat(addr, sname, state)
              [addr, "", @worker, @reply, client_addr, "", req_id | body] ->
                worker_unengaged(addr)
                Ez.Request.response_received(req_id, client_addr, body)
                state
              [_addr, "", @worker, @ack, req_id] ->
                Ez.Request.ack(req_id)
                state
              _ ->
                Logger.warn("didn't recognize #{Kernel.inspect(msg)}")
                state
            end
    {:noreply, state}
  end
  def handle_cast({:worker_engaged, addr},
    state=%Ez.Workers{jobs: jobs}) do
    current_jobs = Map.get(jobs, addr, 0)
    {:noreply, %{state | jobs: Map.put(jobs, addr, current_jobs + 1)}}
  end
  def handle_cast({:worker_unengaged, addr},
    state=%Ez.Workers{jobs: jobs}) do
    current_jobs = Map.get(jobs, addr, 0)
    {:noreply, %{state | jobs: Map.put(jobs, addr, current_jobs - 1)}}
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
    {:noreply, %Ez.Workers{
        addresses: Map.drop(state.addresses, addresses),
        jobs: Map.drop(state.jobs, addresses),
        services: new_services,
        timeouts: Map.drop(state.timeouts, addresses)}
    }
  end

  @impl true
  def handle_call({:get, :timeouts}, _from, state) do
    {:reply, state.timeouts, state}
  end
  def handle_call({:get, :state}, _from, state) do
    {:reply, state, state}
  end

  # private
  defp handle_heartbeat(
    addr, sname,
    state=%Ez.Workers{addresses: addresses,
                      services: services,
                      timeouts: timeouts}) do
    if not Map.has_key?(addresses, addr) do
      Logger.info("new worker for #{sname} - #{Kernel.inspect(addr)}")
    end
    %{state |
      services: Map.put(services, sname,
        MapSet.put(services[sname] || MapSet.new(), addr)),
      addresses: Map.put(addresses, addr, sname),
      timeouts: Map.put(timeouts, addr,
        :os.system_time(:millisecond) + Ez.Env.worker_lifetime())
    }
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
