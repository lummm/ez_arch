defmodule Ez.Request do
  @moduledoc """
  Interface into the EZ framework
  """
  require Logger

  def req(from, req_id, sname, body) do
    spawn(fn ->
      res = do_request(req_id, sname, body, Ez.Env.min_req_timeout())
      # res is of the form {:ok, <response>} or {:err, <reason>}
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
        end
      _ -> Logger.error("Failed to find process response for req id #{req_id}")
    end
  end

  defp do_request(
    req_id, sname, body,
    retry_timeout
  ) do
  # Response is of the form
  # {:ok, result}, {:ez_err, reason}, {:service_err, result}
    Ez.ReqRegistry.put(req_id, self())
    state=%Ez.Workers{} = Ez.Workers.get_state()
    if not Map.has_key?(state.services, sname) do
      Logger.warn("no such service #{sname}")
      if retry_timeout && (retry_timeout < Ez.Env.max_req_timeout()) do
        Process.sleep(retry_timeout)
        do_request(req_id, sname, body, retry_timeout * 2)
      else
        Ez.ReqRegistry.clear(self())
        {:ez_err, :no_service}
      end
    else
      {addr, jobs} = select_worker(state, sname)
      if Ez.Env.backpressure_threshold() < jobs do
        Process.sleep(jobs * Ez.Env.backpressure_ratio())
      end
      Ez.WorkerInterface.send_to_worker(addr, req_id, body)
      receive do
        {:ack, worker_addr} ->
          Ez.Workers.worker_engaged(worker_addr)
          receive do
            {:ok, rest} -> {:ok, rest}
            {:service_err, rest} -> {:service_err, rest}
          end
      after
        retry_timeout ->
          Logger.info("req timeout for #{sname}")
          if retry_timeout < Ez.Env.max_req_timeout() do
            do_request(req_id, sname, body, retry_timeout * 2)
          else
            Ez.ReqRegistry.clear(self())
            {:ez_err, :timeout}
          end
      end

    end
  end

  defp select_worker(state, sname) do
    state.services[sname]
    |> MapSet.to_list()
    |> Stream.map(fn addr -> {addr, Map.get(state.jobs, addr, 0)} end)
    |> Enum.min_by(fn {_addr, jobs} -> jobs end)
  end
end
