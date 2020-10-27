defmodule Ez.Request do
  @moduledoc """
  Interface into the EZ framework
  """
  require Logger

  defstruct req_id: nil, sname: nil, body: nil

  def req(from, req=%Ez.Request{}) do
    spawn(fn ->
      res = do_request(req)
      send(from, res)
    end)
  end

  def ack(req_id, addr) do
    case Ez.ReqRegistry.get(req_id) do
      %{pid: pid} -> send(pid, {:ack, addr})
      _ -> Logger.error("Failed to process ack for req id #{req_id}")
    end
  end

  def response_received(req_id, reply_frames) do
    case Ez.ReqRegistry.get(req_id) do
      %{pid: pid} ->
        case reply_frames do
          ["OK" | rest] -> send(pid, {:ok, rest})
          ["ERR" | rest] -> send(pid, {:service_err, rest})
          _ ->
            Logger.error("bad reply frames: #{Kernel.inspect(reply_frames)}")
        end
      _ -> Logger.error("Failed to find process response for req id #{req_id}")
    end
  end

  defp do_request(req=%Ez.Request{req_id: req_id}) do
    Ez.ReqRegistry.put(req_id, self())
    res = Ez.RequestStateMachine.run(req)
    Ez.ReqRegistry.clear(self())
    res
  end
end
