defmodule Ez.WorkerListen do
  use GenServer

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [nil], name: __MODULE__)
  end

  def worker_msg(msg) do
    GenServer.cast(__MODULE__, {:worker_msg, msg})
  end

  def send_to_worker(addr, req_id, body) do
    GenServer.cast(__MODULE__, {:send, addr, req_id, body})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    worker_port = Ez.Env.worker_port()
    {:ok, socket} = :chumak.socket(:router)
    {:ok, _bind_pid} = :chumak.bind(socket, :tcp, '0.0.0.0', worker_port)
    spawn_link(fn ->
      IO.puts("listening for worker upstream msgs at #{worker_port}")
      listen(socket)
    end)
    {:ok, %{router: socket}}
  end

  @impl true
  def handle_cast({:worker_msg, msg}, state) do
    Ez.Workers.worker_msg(msg)
    {:noreply, state}
  end
  def handle_cast({:send, addr, req_id, body}, state) do
    :chumak.send_multipart(state.router, [addr, "", req_id] ++ body)
    {:noreply, state}
  end

  defp listen(socket) do
    {:ok, msg} = :chumak.recv_multipart(socket)
    worker_msg(msg)
    listen(socket)
  end

end
