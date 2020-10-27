defmodule Ez.RequestStateMachine do
  require Logger

  @doc """
  Response is of the form
  {:ok, result}, {:ez_err, reason}, {:service_err, result}
  """
  def run(req=%Ez.Request{}) do
    load_workers(req, Ez.Env.min_req_timeout())
  end

  defp load_workers(req=%Ez.Request{}, timeout) do
    workers=%Ez.Workers{} = Ez.Workers.get_state()
    find_worker(workers, req, timeout)
  end

  defp find_worker(
    workers=%Ez.Workers{services: services},
    req=%Ez.Request{sname: sname},
    timeout
  ) do
    if not Map.has_key?(services, sname) do
      if timeout && (timeout < Ez.Env.max_req_timeout()) do
        Process.sleep(timeout)
        load_workers(req, timeout * 2)
      else
        Logger.warn("no such service #{sname}")
        {:ez_err, :no_service}
      end
    else
      {addr, jobs} = Ez.Workers.select_worker(workers, sname)
      send_work(req, {addr, jobs}, timeout)
    end
  end

  defp send_work(req=%Ez.Request{req_id: req_id,
                                    body: body},
    {addr, jobs}, timeout
  ) do
    if Ez.Env.backpressure_threshold() < jobs do
      Process.sleep(jobs * Ez.Env.backpressure_ratio())
    end
    Ez.WorkerInterface.send_to_worker(addr, req_id, body)
    wait_ack(req, timeout)
  end

  defp wait_ack(req, timeout) do
    # A timeout means all workers are too busy to take our work.
    # In this case we load the worker state again.
    receive do
      {:ack, worker_addr} ->
        Ez.Workers.worker_engaged(worker_addr)
        wait_res(timeout)
    after
      timeout ->
      if timeout < Ez.Env.max_req_timeout() do
        load_workers(req, timeout * 2)
      else
        {:ez_err, :timeout}
      end
    end
  end

  defp wait_res(timeout) do
    # At this point, the request is being worked on, so we can't resend it.
    receive do
      {:ok, rest} -> {:ok, rest}
      {:service_err, rest} -> {:service_err, rest}
    after
      timeout ->
      if timeout < Ez.Env.max_req_timeout() do
        wait_res(timeout * 2)
      else
        {:ez_err, timeout}
      end
    end
  end

end
