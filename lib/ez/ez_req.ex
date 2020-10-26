defmodule Ez.EzReq do
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

  defp do_request(
    req_id, return_addr, sname, body,
    retry_timeout
  ) do
    state=%Ez.Workers{} = Ez.Workers.get_state()
    if not Map.has_key?(state.services, sname) do
      Logger.warn("no such service #{sname}")
      if retry_timeout && (retry_timeout < Ez.Env.max_req_timeout()) do
        Process.sleep(retry_timeout)
        do_request(req_id, return_addr, sname, body,
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
        :ack -> Ez.Workers.worker_engaged(addr)
      after
        retry_timeout ->
          Logger.info("req timeout for #{sname}")
          if retry_timeout < Ez.Env.max_req_timeout() do
            do_request(req_id, return_addr, sname, body,
              retry_timeout * 2)
          else
              Ez.ZmqReq.reply(return_addr, req_id,
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
