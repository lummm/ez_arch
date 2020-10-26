defmodule Ez.Request do
  @moduledoc """
  Interface into the EZ framework
  """
  require Logger

  def req(req_id, return_addr, sname, body) do
    spawn(fn ->
      do_request(req_id, return_addr, sname, body,
        Ez.Env.min_req_timeout())
    end)
  end

  def ack(req_id) do
    case Ez.Requests.get(req_id) do
      %{pid: pid} -> send(pid, :ack)
      _ -> Logger.error("Failed to process ack for req id #{req_id}")
    end
  end

  def response_received(req_id, client_addr, reply_frames) do
    case Ez.Requests.get(req_id) do
      %{pid: pid} ->
        send(pid, {:response, client_addr, reply_frames})
      _ -> Logger.error("Failed to find process response for req id #{req_id}")
    end
  end

  defp do_request(
    req_id, return_addr, sname, body,
    retry_timeout
  ) do
    Ez.Requests.put(req_id, self())
    state=%Ez.Workers{} = Ez.Workers.get_state()
    if not Map.has_key?(state.services, sname) do
      Logger.warn("no such service #{sname}")
      if retry_timeout && (retry_timeout < Ez.Env.max_req_timeout()) do
        Process.sleep(retry_timeout)
        do_request(req_id, return_addr, sname, body,
          retry_timeout * 2)
      else
        Ez.Requests.clear(self())
        Ez.ZmqInterface.reply(return_addr, req_id,
          ["ERR", "\"no such service\""])
      end
    else
      {addr, jobs} = select_worker(state, sname)
      if Ez.Env.backpressure_threshold() < jobs do
        Process.sleep(jobs * Ez.Env.backpressure_ratio())
      end
      Ez.WorkerListen.send_to_worker(
        addr, req_id, return_addr, body)
      receive do
        :ack ->
          Ez.Workers.worker_engaged(addr)
          receive do
            {:response, client_addr, reply_frames} ->
              Ez.ZmqInterface.reply(client_addr, req_id, reply_frames)
          end
      after
        retry_timeout ->
          Logger.info("req timeout for #{sname}")
          if retry_timeout < Ez.Env.max_req_timeout() do
            do_request(req_id, return_addr, sname, body,
              retry_timeout * 2)
          else
            Ez.Requests.clear(self())
            Ez.ZmqInterface.reply(return_addr, req_id,
              ["ERR", "\"timeout\""])
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
