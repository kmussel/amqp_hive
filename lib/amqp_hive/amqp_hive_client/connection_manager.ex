defmodule AmqpHiveClient.ConnectionManager do
  use GenServer
  use AMQP
  require Logger

  @time 5_000

  def start_link(conn) do
    GenServer.start_link(__MODULE__, conn, name: __MODULE__)
  end

  def child_spec(conn) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [conn]}
    }
  end

  def init(connections) do
    GenServer.cast(self(), :setup_connections)
    registry = %{}
    refs = %{}
    {:ok, {connections, registry, refs}}
  end

  def start_connections_and_consumers(connections) do
    Enum.each(connections, fn conn ->
      GenServer.cast(self(), {:start_connection, conn})
    end)
  end

  def remove_connection(name) do
    GenServer.cast(__MODULE__, {:remove_connection, name})
  end

  def add_connection(conn) do
    # conn = %{connection: %{host: "localhost", password: "guest", port: 5672, username: "guest"}, consumers: [%{queue: "my_queue"}], name: "queue_handler"}
    GenServer.cast(__MODULE__, {:add_connection, conn})
  end

  @spec random_consumer_name(integer) :: binary
  def random_consumer_name(bytes_count) do
    SecureRandom.hex(bytes_count)
  end

  def start_consumers(%{name: conn_name} = conn) do
    consumers = Map.get(conn, :consumers, [])

    Enum.each(consumers, fn consumer ->
      name = Map.get(consumer, :name, random_consumer_name(10))
      GenServer.cast(self(), {:start_consumer, {conn_name, name, consumer}})
    end)
  end

  def start_producers(%{name: conn_name} = conn) do
    producers = Map.get(conn, :producers, [%{cnt: 1}])

    Enum.each(producers, fn producer ->
      Map.get(producer, :cnt, 1)
      name = Map.get(producer, :name, random_consumer_name(10))
      GenServer.cast(self(), {:start_producer, {conn_name, name, producer}})
    end)
  end

  # defp event(delay) do
  #   me = self()
  #   %Timer.Event{
  #     id: "start_connections",
  #     handler: fn -> GenServer.cast(me, :start_connections) end,
  #     interval: delay
  #   }
  # end

  def register_consumer(name, consumer, conn_name) do
    # Logger.info("Register New Consumer #{name}")
    # Process.whereis(AmqpHiveClient.ConnectionSupervisor) 
    # GenServer.cast(AmqpHiveClient.ConnectionManager, {:register_consumer, {name, consumer, conn_name}})
    # handle_call
    with {:ok, _conpid} <- GenServer.call(__MODULE__, {:start_connection_name, conn_name}) do
      {:ok, pid} = AmqpHiveClient.ConsumerSupervisor.start_consumer(consumer, conn_name)
      # Logger.info("The start consumer res = #{inspect(pid)}")
      AmqpHive.ConsumerRegistry.monitor(pid, {name, consumer, conn_name})
      {:ok, pid}
    else
      err ->  {:error, err}
    end
  end
  

  def handle_call({:swarm, :begin_handoff}, _from, state) do
    Logger.info("[SWARM ConnManager] begin_handoff: #{inspect(state)}")
    {:reply, :resume, state}
  end

  def handle_cast({:swarm, :end_handoff}, state) do
    Logger.info("[SWARM ConnManager] begin_handoff: #{inspect(state)}")
    {:noreply, state}
  end

  def handle_cast({:swarm, :resolve_conflict}, state) do
    Logger.info("[SWARM ConnManager] resolve_conflict: #{inspect(state)}")
    {:noreply, state}
  end

  def handle_info({:swarm, :die}, state) do
    Logger.info("[SWARM ConnManager] swarm stopping worker: #{inspect(state)}")
    {:stop, :normal, state}
  end


  def handle_cast(:setup_connections, {connections, registry, refs} = _state) do
    Logger.info(fn -> "Setup Connections #{inspect(connections)}" end)

    new_connections =
      Enum.reduce(connections, [], fn conn, acc ->
        conn_name = Map.get(conn, :name, "")

        consumers =
          Enum.reduce(Map.get(conn, :consumers, []), [], fn consumer, acc_consumer ->
            consumer_name = "#{conn_name}-#{Map.get(consumer, :name, random_consumer_name(10))}"
            acc_consumer ++ [Map.put(consumer, :name, consumer_name)]
          end)
        conn = Map.put(conn, :consumers, consumers)          

        producers =
          Enum.reduce(Map.get(conn, :producers, [%{cnt: 1}]), [], fn producer, acc_producer ->
            producer_name = "#{conn_name}-#{Map.get(producer, :name, random_consumer_name(10))}"
            acc_producer ++ [Map.put(producer, :name, producer_name)]
          end)  
        conn = Map.put(conn, :producers, producers)
        acc ++ [conn]
      end)

    Process.send_after(self(), :start_connections, @time + :rand.uniform(1000))
      # spawn_link(&(AmqpHiveClient.ConnectionManager.start_connections))
    # Timer.add(:connection_manager_timer, event(@time + :rand.uniform(1000)))
    {:noreply, {new_connections, registry, refs}}
  end


  def handle_info(:start_connections, {connections, _registry, _refs} = state) do
    start_connections_and_consumers(connections)
    Process.send_after(self(), :start_connections, @time + :rand.uniform(1000))
    {:noreply, state}
  end

  def handle_cast({:add_connection, conn}, {connections, registry, refs} = _state) do
    Logger.info(fn -> "Adding New Connection #{inspect(conn)}" end)
    newconnections = connections ++ [conn]
    {:noreply, {newconnections, registry, refs}}
  end

  def handle_cast({:remove_connection, name}, {connections, registry, refs} = state) do
    Logger.info(fn -> "Connection List Remove Connection: = #{inspect(name)}" end)

    case Enum.find(connections, &(get_in(&1, [:name]) == name)) do
      nil ->
        {:noreply, state}

      conn ->
        newconnections = List.delete(connections, conn)
        {:noreply, {newconnections, registry, refs}}
    end
  end

  def handle_cast({:start_connection, %{name: _name} = conn}, {_, _, _} = state) do
    case start_connection(conn, state) do
      {:ok, _pid, newstate} -> 
        {:noreply, newstate}
      {:error, _other, newstate} -> 
        {:noreply, newstate}
    end
  end

  def handle_call({:start_connection_name, name}, _from, {connections, _, _} = state) do
    case Enum.find(connections, &(get_in(&1, [:name]) == name)) do
      nil ->  {:reply, {:error, "No connection with that name"}, state}
      conn -> 
        case start_connection(conn, state) do
          {:ok, pid, newstate} -> 
            {:reply, {:ok, pid}, newstate}
          {:error, other, newstate} -> 
            {:reply, {:error, other}, newstate}
        end
    end
  end

  def handle_cast({:start_consumer, {conn_name, consumer_name, consumer}}, {connections, names, refs}) do

    if Map.has_key?(names, consumer_name) do
      {:noreply, {connections, names, refs}}
    else
      # Logger.info(fn -> "Starting Consumer #{inspect(consumer)}" end)

      case AmqpHiveClient.ConsumerSupervisor.start_consumer(consumer, conn_name) do
        {:ok, pid} ->
          monitor_process({consumer_name, pid}, {connections, names, refs})

        {:error, {:already_started, pid}} ->
          # Logger.info("Consumer Already Started #{inspect(consumer_name)}")
          monitor_process({consumer_name, pid}, {connections, names, refs})

        other ->
          Logger.info(fn -> "Start Consumer Failed:  #{inspect(other)}" end)
          {:noreply, {connections, names, refs}}
      end
    end
  end

  def handle_cast({:start_producer, {conn_name, producer_name, producer}}, {connections, names, refs}) do

    if Map.has_key?(names, producer_name) do
      {:noreply, {connections, names, refs}}
    else
      Logger.info(fn -> "Starting Producer #{inspect(producer_name)}" end)

      case AmqpHiveClient.ProducerSupervisor.start_producer(producer, conn_name) do
        {:ok, pid} ->
          monitor_process({producer_name, pid}, {connections, names, refs})

        {:error, {:already_started, pid}} ->
          Logger.info("Producer Already Started #{inspect(producer_name)}")
          monitor_process({producer_name, pid}, {connections, names, refs})

        other ->
          Logger.info(fn -> "Start Producer Failed:  #{inspect(other)}" end)
          {:noreply, {connections, names, refs}}
      end
    end
  end

  def handle_cast(
        {:remove_consumer, consumer_name, connection_name},
        {connections, registry, refs} = _state
      ) do
    Logger.info(fn -> "Connection List Remove Consumer: = #{inspect(consumer_name)}" end)

    newconnections =
      Enum.reduce(connections, [], fn conn, new_conns ->
        if Map.get(conn, :name) == connection_name do
          current_consumers = Map.get(conn, :consumers, [])

          consumers =
            case Enum.find(current_consumers, &(get_in(&1, [:name]) == consumer_name)) do
              nil ->
                current_consumers

              consumer ->
                List.delete(current_consumers, consumer)
            end

          new_conns ++ [Map.put(conn, :consumers, consumers)]
        else
          new_conns ++ [conn]
        end
      end)

    pid = Map.get(registry, consumer_name)
    AmqpHiveClient.ConsumerSupervisor.stop_consumer(connection_name, pid)
    {:noreply, {newconnections, registry, refs}}
  end

  def handle_cast(_other, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {connections, names, refs} = _state) do
    {name, refs} = Map.pop(refs, ref)
    Logger.info(fn -> "[ConnectionManager] Process Down: name = #{inspect(name)}" end)
    names = Map.delete(names, name)
    {:noreply, {connections, names, refs}}
  end

  def handle_info(msg, state) do
    Logger.info(fn -> "HANDLE CONN WORKER INFO: reason = #{inspect(msg)}" end)
    {:noreply, state}
  end

  defp monitor_process({name, pid}, {connections, names, refs}) do
    Logger.info(fn -> "[CONNECTION_MANAGER] Monitor Process:  #{inspect(name)} with pid #{inspect(pid)}" end)
    Logger.info(fn -> "[CONNECTION_MANAGER] Monitor Process state:  #{inspect(names)}" end)
    ref = Process.monitor(pid)
    newrefs = Map.put(refs, ref, name)
    newnames = Map.put(names, name, pid)
    Logger.info(fn -> "[CONNECTION_MANAGER] Monitor Process new state names:  #{inspect(newnames)}" end)
    {:noreply, {connections, newnames, newrefs}}
  end

  
  defp start_connection(%{name: name} = conn, {connections, names, refs} = state) do
    if Map.has_key?(names, name) do
      start_consumers(conn)
      start_producers(conn)
      pid = Map.get(names, name)
      {:ok, pid, state}
    else
      Logger.info(fn -> "Creating Single Connection #{inspect(conn)}" end)

      case AmqpHiveClient.ConnectionSupervisor.start_connection(conn) do
        {:ok, pid} ->
          start_consumers(conn)
          start_producers(conn)
          {:noreply, newstate} = monitor_process({name, pid}, {connections, names, refs})
          {:ok, pid, newstate}

        {:error, {:already_started, pid}} ->
          {:noreply, newstate} = monitor_process({name, pid}, {connections, names, refs})
          {:ok, pid, newstate}
          

        other ->
          Logger.info(fn -> "Start Connection Failed:  #{inspect(other)}" end)
          {:error, other, state}
      end
    end
  end
end
