defmodule AmqpHive.ConsumerRegistry do
  use GenServer
  require Logger

  @name __MODULE__
  @swarm_group :amqp_hive_consumers


  def child_spec() do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    }
  end

  def start_link do
    GenServer.start_link(__MODULE__, [], name: @name)
  end

  def register(name) do
    GenServer.call(@name, {:register, name})
  end

  def register_consumer(name, consumer, connection_name) do
    GenServer.call(@name, {:register_consumer, name, consumer, connection_name})
  end

  def monitor(pid, {consumer_name, consumer, conn_name}) do
    GenServer.cast(@name, {:monitor, pid, {consumer_name, consumer, conn_name}})
  end

  def log_state() do
    send(Process.whereis(@name), :log_state)
  end

  def init(_) do
    Process.send_after(self(), :handle_startup, 10000)
    Process.flag(:trap_exit, true)
    {:ok, %{}}
  end

  def handle_cast({:monitor, pid, consumer}, state) do
    ref = Process.monitor(pid)
    {:noreply, Map.put(state, ref, consumer)}
  end

  def handle_call({:register_consumer, name, consumer, connection_name}, _from, state) do
    case start_consumer_via_swarm(name, consumer, connection_name) do
      {:ok, pid} ->        
        {:reply, {:ok, pid}, state}
      {:already_registered, pid} ->
        {:reply, {:ok, pid}, state}
      {:error, reason} ->
        Logger.error("[Registry] error starting #{name} - #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  def handle_info(:handle_startup, state) do
    module =
      case Application.get_env(:amqp_hive, :registry_handler) do
        nil -> AmqpHiveClient.Handlers.Registry
        ro -> ro
      end

    apply(module, :handle_startup, [])
    {:noreply, state}
  end

  def handle_info(:log_state, state) do
    total = Swarm.registered() |> Enum.count()
    local = state |>  Enum.count()

    Logger.debug("[Registry] Totals: #{inspect(get_swarm_state())}  Swarm/#{total} Local/#{local}")
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
    # Logger.debug("[Registry] DOWN NORMAL #{inspect(Map.get(state, ref))}")
    case Map.get(state, ref) do
      {name, _consumer, _conn_name} ->
        Swarm.unregister_name(name)
      _other -> nil
    end
    {:noreply, Map.delete(state, ref)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    # Logger.debug("[Registry] DOWN #{reason} #{inspect(Map.get(state, ref))}")
    case Map.get(state, ref) do
      nil ->
        {:noreply, state}
      {name, consumer, conn_name} ->
        Swarm.unregister_name(name)
        {:ok, _pid} = start_consumer_via_swarm(name, consumer, conn_name, "restarting")
        {:noreply, Map.delete(state, ref)}
      other -> 
        Logger.debug("[Registry] :DOWN #{inspect(other)}")
          {:noreply, Map.delete(state, ref)}
    end
  end

  def start_consumer_via_swarm(name, consumer, connection_name, _reason \\ "starting") do
    with :undefined <- Swarm.whereis_name(name),
         {:ok, pid} <- Swarm.register_name(name, AmqpHiveClient.ConnectionManager, :register_consumer, [name, consumer, connection_name]) do
          Swarm.join(@swarm_group, pid)
      {:ok, pid}
    else
      pid when is_pid(pid) ->
        Swarm.join(@swarm_group, pid)
        {:ok, pid}

      {:error, {:already_registered, pid}} ->
        Swarm.join(@swarm_group, pid)
        {:ok, pid}

      {:error, som} = err ->
        Logger.info("[Registry] start_consumer error #{inspect(som)}")
        err
    end
  end

  def get_swarm_state(), do: :sys.get_state(Swarm.Tracker) #Swarm.members(@swarm_group)
end