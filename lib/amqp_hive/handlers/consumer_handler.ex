defmodule AmqpHiveClient.Handlers.Consumer do  
  use AMQP
  require Logger


  def consume(pid, channel, payload, meta, state) do
    Logger.info(fn -> "Consumer Meta is #{inspect(meta)}" end)

    response = 
      case handle_route(pid, payload, meta, state) do
        %{"error" => msg} ->
          :ok = Basic.reject channel, meta.delivery_tag, requeue: not meta.redelivered
          %{"error" => msg}
        response ->  
          AMQP.Basic.ack(channel, meta.delivery_tag)
          response
      end
    
    case meta.reply_to do
      r when is_nil(r) or r == :undefined ->
        nil

      reply_to ->
        AMQP.Basic.publish(
          channel,
          "",
          reply_to,
          "#{Poison.encode!(response)}",
          correlation_id: meta.correlation_id
        )
    end
  rescue
    # Requeue unless it's a redelivered message.
    # This means we will retry consuming a message once in case of exception
    # before we give up and have it moved to the error queue
    #
    # You might also want to catch :exit signal in production code.
    # Make sure you call ack, nack or reject otherwise comsumer will stop
    # receiving messages.
    exception ->      
      Logger.error(fn -> "Error consume #{inspect(exception)}" end)
      :ok = Basic.reject channel, meta.delivery_tag, requeue: not meta.redelivered
      Logger.error(fn -> "Error consuming payload: #{payload}" end)
  end

  def handle_route(_pid, payload, %{routing_key: "rpc.create_deployment"} = _meta, {_consumer, _other, %{parent: connection_name}}) do
    context = Poison.decode!(payload)
    case Map.get(context, "deployment_id") do
      nil ->  %{"error" => "No Deployment ID" }
      dep_id -> 
        name = "#{connection_name}-#{dep_id}"
        consumer = %{name: dep_id, queue: dep_id}

        AmqpHive.ConsumerRegistry.register_consumer(name, consumer, connection_name)
        %{"success" =>  "Waiting for Deployment "}
    end    
  rescue 
    exception -> 
      Logger.error(fn -> "Error handle_route rpc.create_deployment #{inspect(exception)}" end)
      %{"error" => "Error Creating Addons for #{payload}"}
  end

  def handle_route(pid, payload, meta, state) do
    # Logger.info("handle route #{inspect(state)}")    
    body = Poison.decode!(payload)
    handle_action(pid, body, meta, state)
  rescue 
    _exception -> 
      Logger.error(fn -> "Error converting #{payload} to json" end)
      %{"error" => "Error converting #{payload} to json"}
  end

  def handle_action(pid, %{"action" => "finished"} = _payload, _meta, _state) do    
    # Logger.info("Handle finish action #{inspect(pid)}")
    GenServer.cast(pid, :finished)
    %{"success" => "Finished"}
  end

  def handle_action(_pid, %{"action" => "log", "msg" => _msg} = _payload, _meta, _state) do    
    # Logger.info("Handle LOG action #{inspect(msg)}")
    %{"success" => "Logged"}
  end

  def handle_action(_pid, _payload, _meta, _state) do
    %{"error" => "No Action Handler"}
  end
end