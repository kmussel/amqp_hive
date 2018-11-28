defmodule AmqpHiveClient.Producer do
  use GenServer
  use AMQP
  require Logger

  def start_link(producer, connection_name) do
    GenServer.start_link(__MODULE__, {producer, connection_name})
  end

  def child_spec(consumer, connection_name) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [consumer, connection_name]},
      restart: :temporary
    }
  end

  def init({producer, connection_name}) do
    AmqpHiveClient.Connection.request_channel(connection_name, self())
    {:ok, {producer, nil, %{parent: connection_name}}}
  end

  def handle_call({:publish, route, payload, options}, _from, {_producer, channel, _conn_name} = state) do
    exchange_name = Keyword.get(options, :exchange, "")
    pub = AMQP.Basic.publish(
          channel,
          exchange_name,
          route,
          "#{Poison.encode!(payload)}",
          options
        )
    {:reply, pub, state}
  end

  def handle_call({:swarm, :begin_handoff}, _from, state) do
    Logger.info("[PRODUCER SWARM] BEGIN HANDoff #{inspect(state)}")
    {:reply, {:resume, 2000}, state}
  end
  # called after the process has been restarted on its new node,
  # and the old process' state is being handed off. This is only
  # sent if the return to `begin_handoff` was `{:resume, state}`.
  # **NOTE**: This is called *after* the process is successfully started,
  # so make sure to design your processes around this caveat if you
  # wish to hand off state like this.
  def handle_cast({:swarm, :end_handoff, _delay}, state) do
    Logger.info("[PRODUCER SWARM] END HANDoff #{inspect(state)}")
    {:noreply, state}
  end
  # called when a network split is healed and the local process
  # should continue running, but a duplicate process on the other
  # side of the split is handing off its state to us. You can choose
  # to ignore the handoff state, or apply your own conflict resolution
  # strategy
  def handle_cast({:swarm, :resolve_conflict, _delay}, state) do
    Logger.info("[PRODUCER SWARM] Resolve conflicts #{inspect(state)}")
    {:noreply, state}
  end

  def handle_cast({:channel_available, chan}, {producer, _channel, other} = _state) do
    Process.monitor(chan.pid)

        
    # {:ok, _consumer_tag} = Basic.consume(chan, queue)
    {:noreply, {producer, chan, other}}
  end

  def handle_cast(
        {:stop, reason},
        {_producer, _chan, %{parent: _connection_name} = _options} = state
      ) do
    Logger.debug(fn -> "Handle Stop Consumer CAST: #{inspect(reason)}" end)

    {:noreply, state}
  end

  def handle_cast(:finished, {_consumer, _channel, _} = state) do
    Logger.debug(fn -> "[PRODUCER] HANDLE Cast Finished Queue = #{inspect(state)}" end)    
    # AMQP.Channel.close(channel)
    # Process.exit(self(), :normal)
    {:noreply, state}
  end

  def handle_cast(other, state) do
    Logger.debug(fn -> "Un-Handled Consumer CAST: #{inspect(other)}" end)
    {:noreply, state}
  end

  def handle_info(:timeout, {name, delay}) do
    Logger.info("[PRODUCER Timeout] #{inspect(name)}")
    Process.send_after(self(), :timeout, delay)
    {:noreply, {name, delay}}
  end
  # this message is sent when this process should die
  # because it is being moved, use this as an opportunity
  # to clean up
  def handle_info({:swarm, :die}, state) do
    Logger.info("[PRODUCER SWARM] DIE #{inspect(state)}")
    {:stop, :shutdown, state}
  end
  

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, chan) do
    Logger.debug(fn -> "[PRODUCER] basic consume #{inspect(consumer_tag)}" end)
    {:noreply, chan}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: consumer_tag}}, chan) do
    Logger.debug(fn -> "[PRODUCER] basic cancel #{inspect(consumer_tag)}" end)
    {:stop, :normal, chan}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: consumer_tag}}, chan) do
    Logger.debug(fn -> "[PRODUCER] basic cancel_ok #{inspect(consumer_tag)}" end)
    {:noreply, chan}
  end

  def handle_info({:basic_deliver, payload, _meta}, {_consumer, _chan, _other} = state) do
    pid = self()
    Logger.debug(fn -> "[PRODUCER] Basic deliver in #{inspect(pid)} #{inspect(payload)}" end)
    {:noreply, state}
  end

  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.debug(fn -> "[PRODUCER] Consumer Down, reason: #{inspect(reason)} state = #{inspect(state)}" end)
    {:noreply, state}
  end

  def handle_info(
        {:EXIT, _, {:shutdown, {:connection_closing, {:server_initiated_close, _, reason}}}},
        state
      ) do
    Logger.debug(fn -> "[PRODUCER] HANDLE EXIT: reason = #{inspect(reason)} : State= #{inspect(state)}" end)
    {:noreply, state}
  end

  def handle_info({:EXIT, _, reason}, state) do
    Logger.debug(fn -> "[PRODUCER] EXIT REASON #{inspect(reason)}" end)
    {:stop, reason, state}
  end

  def handle_info(:stop, state) do
    Logger.debug(fn -> "[PRODUCER] HANDLE kill channel state = #{inspect(state)}" end)
    GenServer.cast(self(), {:stop, "stopme"})
    {:noreply, state}
  end

  def handle_info(:kill_channel, {_, channel, _} = state) do
    Logger.debug(fn -> "HANDLE kill channel state = #{inspect(state)}" end)
    AMQP.Channel.close(channel)
    {:noreply, state}
  end

  def handle_info(:finished, {_consumer, channel, _} = state) do
    Logger.debug(fn -> "[PRODUCER] HANDLE Finished Queue = #{inspect(state)}" end)    
    AMQP.Channel.close(channel)
    Process.exit(self(), :normal)
    {:noreply, state}
  end

  def handle_info(reason, state) do
    Logger.info(fn -> "[PRODUCER] UN-HANDLE INFO: reason = #{inspect(reason)} state = #{inspect(state)}" end)
    {:noreply, state}
  end

  def terminate(other, {_consumer, channel, _options} = state) do
    Logger.debug(fn -> "[PRODUCER TERMINATE] other = #{inspect(other)} and stuff = #{inspect(state)}" end)
    AMQP.Channel.close(channel)
    :shutdown
  end

  def channel_available(pid, chan) do
    GenServer.cast(pid, {:channel_available, chan})
  end
  
  def publish(connection_name, route, payload, options \\ []) do
    {:ok, pid} = AmqpHiveClient.ProducerSupervisor.get_available_producer(connection_name)
    res = GenServer.call(pid, {:publish, route, payload, options}, 30_000)
    res
  rescue
    exception ->      
      Logger.error(fn -> "Error publishing: #{inspect(exception)} : payload: #{payload}" end)
  end
end
