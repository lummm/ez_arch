defmodule Ez.ZmqInterface do
  @moduledoc """
  This is the interface to requests over Zmq.
  Incoming requests are interpreted as frames, and then sent to the EzReq
  server to be handled.
  The Zmq request is 'fire-and-forget', and the response is then waited for.
  Thus we use 'handle_cast' in this server.
  """
  use GenServer
  require Logger

  @client <<2>>

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def zmq_req(msg) do
    GenServer.cast(__MODULE__, {:zmq_req, msg})
  end

  def reply(frames) do
    GenServer.cast(__MODULE__, {:reply, frames})
  end
  def reply(return_addr, req_id, body) do
    frames = [return_addr, "", req_id] ++ body
    GenServer.cast(__MODULE__, {:reply, frames})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    port = Ez.Env.ez_port()
    {:ok, socket} = :chumak.socket(:router)
    {:ok, _bind_pid} = :chumak.bind(socket, :tcp, '0.0.0.0', port)
    spawn_link(fn ->
      Logger.info("listening for EZ requests at #{port}")
      listen(socket)
    end)
    {:ok, %{router: socket}}
  end

  @impl true
  def handle_cast({:zmq_req, msg}, state) do
    case msg do
      [return_addr, "", @client, req_id, service_name | rest] ->
        Ez.EzReq.req(req_id, return_addr, service_name, rest)
        {:noreply, state}
      _ ->
        Logger.warn("bad zmq req #{Kernel.inspect(msg)}")
        {:noreply, state}
    end
  end
  def handle_cast({:reply, frames}, state) do
    :chumak.send_multipart(state.router, frames)
    {:noreply, state}
  end

  defp listen(socket) do
    {:ok, msg} = :chumak.recv_multipart(socket)
    zmq_req(msg)
    listen(socket)
  end

end
