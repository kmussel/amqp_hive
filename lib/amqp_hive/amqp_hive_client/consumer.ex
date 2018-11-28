defmodule AmqpHiveClient.Consumer do
  use GenServer
  use AMQP
  require Logger

  def start_link(consumer, connection_name) do
    GenServer.start_link(__MODULE__, {consumer, connection_name})
  end

  def child_spec(consumer, connection_name) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [consumer, connection_name]},
      restart: :temporary
    }
  end

  def init({consumer, connection_name}) do
    # res = AmqpHiveClient.Connection.request_channel(connection_name, self())
    Process.send_after(self(), :ensure_channel, 5_000)
    {:ok, {consumer, nil, %{parent: connection_name}}}
  end

  def handle_call({:swarm, :begin_handoff}, _from, state) do
    # Logger.info("[SWARM] BEGIN HANDoff #{inspect(state)}")
    # {:reply, {:resume, 2000}, state}
    {:reply, :ignore, state}
  end

  def handle_call(other, _from, state) do
    Logger.info("[CONSUMER HANDLE CALL] #{inspect(other)}")
    {:reply, :ok, state}
  end
  # called after the process has been restarted on its new node,
  # and the old process' state is being handed off. This is only
  # sent if the return to `begin_handoff` was `{:resume, state}`.
  # **NOTE**: This is called *after* the process is successfully started,
  # so make sure to design your processes around this caveat if you
  # wish to hand off state like this.
  def handle_cast({:swarm, :end_handoff, _delay}, state) do
    Logger.info("[SWARM] END HANDoff #{inspect(state)}")
    {:noreply, state}
  end
  # called when a network split is healed and the local process
  # should continue running, but a duplicate process on the other
  # side of the split is handing off its state to us. You can choose
  # to ignore the handoff state, or apply your own conflict resolution
  # strategy
  def handle_cast({:swarm, :resolve_conflict, _delay}, state) do
    Logger.info("[SWARM] Resolve conflicts #{inspect(state)}")
    {:noreply, state}
  end

  def handle_cast({:channel_available, chan}, {consumer, _channel, attrs} = _state) do
    Process.monitor(chan.pid)

    queue = Map.get(consumer, :queue, "")
    queue_options = Map.get(consumer, :options, [durable: true])
    prefetch_count = Map.get(consumer, :prefetch_count, 10)

    Basic.qos(chan, prefetch_count: prefetch_count)
    AMQP.Queue.declare(chan, queue, queue_options)
    {:ok, consumer_tag} = Basic.consume(chan, queue)
    newattrs = Map.put(attrs, :consumer_tag, consumer_tag)
    {:noreply, {consumer, chan, newattrs}}
  end

  def handle_cast(
        {:stop, reason},
        {_consumer, _chan, %{parent: _connection_name} = _options} = state
      ) do
    Logger.debug(fn -> "Handle Stop Consumer CAST: #{inspect(reason)}" end)
    # consumer_name = Map.get(consumer, :name)

    # res =
    #   GenServer.cast(
    #     AmqpHiveClient.ConnectionManager,
    #     {:remove_consumer, consumer_name, connection_name}
    #   )

    {:noreply, state}
  end

  def handle_cast(:finished, {consumer, channel, %{consumer_tag: _tag}} = state) do
    # Logger.debug(fn -> "[CONSUMER] HANDLE Cast Finished Queue = #{inspect(state)}" end)
    case channel do
      nil -> 
        {:stop, :normal, state}
      chan -> 
        if Process.alive?(chan.pid) do      
          case Map.get(consumer, :queue) do
            nil -> nil
            queue -> 
               GenServer.cast(AmqpHiveClient.QueueHandler, {:delete_queue, chan, queue})
          end
        end
        {:noreply, state}
        # {:stop, :normal, state}
    end
  end

  def handle_cast(other, state) do
    Logger.debug(fn -> "Un-Handled Consumer CAST: #{inspect(other)} #{inspect(state)}" end)
    {:noreply, state}
  end

  def handle_info(:timeout, state) do
    Logger.info("[Timeout] #{inspect(state)}")
    # Process.send_after(self(), :timeout, delay)
    {:noreply, state}
  end
  # this message is sent when this process should die
  # because it is being moved, use this as an opportunity
  # to clean up
  def handle_info({:swarm, :die}, state) do
    Logger.info("[SWARM] DIE #{inspect(state)}")
    {:stop, :shutdown, state}
  end
  
  def handle_info(:ensure_channel, {_consumer, channel, %{parent: connection_name}} = state) do
    if is_nil(channel) do
      AmqpHiveClient.Connection.request_channel(connection_name, self())
      # Process.send_after(self(), :ensure_channel, 5_000)
    end
    {:noreply, state}
  end


  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, {consumer, chan, attrs} = _state) do
    # Logger.debug(fn -> "[CONSUMER] basic consume #{inspect(consumer_tag)}" end)
    newattrs = Map.put(attrs, :consumer_tag, consumer_tag)
    {:noreply, {consumer, chan, newattrs}}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, {_consumer, _chan, _attrs} = state) do
    # Logger.debug(fn -> "[CONSUMER] basic cancel #{inspect(consumer_tag)}" end)

    {:stop, :normal, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, {_consumer, _chan, _attrs} = state) do
    # Logger.debug(fn -> "[CONSUMER] basic cancel_ok #{inspect(consumer_tag)}" end)
    {:stop, :normal, state}
  end

  # Receives and processes message from queue
  def handle_info({:basic_deliver, payload, meta}, {_consumer, chan, _other} = state) do
    pid = self()
    Logger.info(fn -> "Basic deliver in #{inspect(pid)} #{inspect(payload)}" end)

    module =
      case Application.get_env(:amqp_hive, :consumer_handler) do
        nil -> AmqpHiveClient.Handlers.Consumer
        ro -> ro
      end

    spawn(fn -> apply(module, :consume, [pid, chan, payload, meta, state]) end)
    {:noreply, state}
  end

  def handle_info({:DOWN, _, :process, _pid, _reason}, state) do
    # Logger.debug(fn -> "Consumer Down, reason: #{inspect(reason)} state = #{inspect(state)}" end)
    {:noreply, state}
  end

  def handle_info(
        {:EXIT, _, {:shutdown, {:connection_closing, {:server_initiated_close, _, reason}}}},
        state
      ) do
    Logger.debug(fn -> "HANDLE EXIT: reason = #{inspect(reason)} : State= #{inspect(state)}" end)
    {:noreply, state}
  end

  def handle_info({:EXIT, _, reason}, state) do
    Logger.debug(fn -> "EXIT REASON #{inspect(reason)}" end)
    {:stop, reason, state}
  end

  def handle_info(:stop, state) do
    # Logger.debug(fn -> "HANDLE kill channel state = #{inspect(state)}" end)
    # GenServer.cast(self(), {:stop, "stopme"})
    {:noreply, state}
  end

  def handle_info(:kill_channel, {_, _channel, _} = state) do
    Logger.debug(fn -> "HANDLE kill channel state = #{inspect(state)}" end)
    # AMQP.Channel.close(channel)
    {:noreply, state}
  end

  def handle_info(:finished, {consumer, channel, _attrs} = state) do
    # Logger.debug(fn -> "HANDLE Finished Queue = #{inspect(state)}" end)
    case channel do
      nil -> 
        {:stop, :normal, state}
      chan -> 
        if Process.alive?(chan.pid) do      
          case Map.get(consumer, :queue) do
            nil -> nil
            queue -> 
              GenServer.cast(AmqpHiveClient.QueueHandler, {:delete_queue, chan, queue})
          end
        end
        {:noreply, state}
    end
  end

  def handle_info(reason, state) do
    Logger.info(fn -> "UN-HANDLE INFO: reason = #{inspect(reason)} state = #{inspect(state)}" end)
    {:noreply, state}
  end


  def terminate(other, {_consumer, _channel, _options} = state) do
    Logger.debug(fn -> "[CONSUMER TERMINATE] other = #{inspect(other)} and stuff = #{inspect(state)}" end)
    :shutdown
  end

  def channel_available(pid, chan) do
    GenServer.cast(pid, {:channel_available, chan})
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
