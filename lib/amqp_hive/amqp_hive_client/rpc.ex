defmodule AmqpHiveClient.Rpc do
  require Logger
  def start_link() do
    Task.start_link(fn -> loop(%{}) end)
  end

  defp loop(map) do
    receive do
      {:received_response, response} ->
        Logger.info("INSIDE response #{inspect(response)}")
        {caller, ref} = Map.get(map, "caller")
        send caller, {ref, response}
        response
      {:send, {caller, ref, [name, callback]}} ->
        Logger.info("INSIDE SEND ARGs")
        callback.()
        loop(Map.put(map, "caller", {caller, ref}))
      other ->
        Logger.info("INSIDE other #{inspect(other)}")        
    end
  end

  def task(registered_name, pid, func, args) do
    ref = :erlang.make_ref()
    task = %Task{owner: self(), pid: pid, ref: ref}    
    Logger.info("REgistered it with pid = #{inspect(pid)}")
    Swarm.send(registered_name, {func, {self(), ref, args}})
    task
  end

  def register_task(name, args) do
    {:ok, pid} = AmqpHiveClient.Rpc.start_link()
    Logger.info("START TASK ok #{inspect(pid)}")
    AmqpHive.RpcRegistry.monitor(pid, {name, args})
    {:ok, pid}
  end

  def publish(conn_name, route, payload, options \\ []) do
    uuid = UUID.uuid4()
    correlation_id = "task-#{uuid}"
    options = options ++ [correlation_id: correlation_id]
    args = [correlation_id, fn -> 
      AmqpHiveClient.Producer.publish(conn_name, route, payload, options)
    end]
    {:ok, pid} = AmqpHive.RpcRegistry.register_task(correlation_id, args)
    task = AmqpHiveClient.Rpc.task(correlation_id, pid, :send, args)
  end
end