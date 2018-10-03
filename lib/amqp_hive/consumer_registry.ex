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
    Process.send_after(self(), :log_state, 25000)
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
    # {:tracking, tracker_state} = get_swarm_state()
    # %Swarm.Tracker.TrackerState{}
    # if Enum.count(tracker_state.nodes) < 1 do
      resubscribe_queues()
    {:noreply, state}
  end

  def handle_info(:log_state, state) do
    total = Swarm.registered() |> Enum.count()
    local = state |>  Enum.count()
    # Logger.debug("[Registry] #{inspect(Swarm.registered())}")
    # Logger.debug("[Local Registry STATE] #{inspect(state)}")
    
    Logger.info("[Registry] Totals: #{inspect(get_swarm_state())}  Swarm/#{total} Local/#{local}")
    Process.send_after(self(), :log_state, 15000)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
    # Logger.debug("[Registry] DOWN NORMAL #{inspect(Map.get(state, ref))}")
    case Map.get(state, ref) do
      {name, consumer, conn_name} ->
        Swarm.unregister_name(name)
      other -> nil
    end
    {:noreply, Map.delete(state, ref)}
  end

  # def handle_info({:DOWN, ref, :process, _pid, :shutdown}, state) do
  #   Logger.debug("[Registry] Shutdown #{inspect(Map.get(state, ref))}")
  #   {:noreply, Map.delete(state, ref)}
  # end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
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

  def start_consumer_via_swarm(name, consumer, connection_name, reason \\ "starting") do
    # Logger.debug("[Registry] #{reason} #{name}")
    with :undefined <- Swarm.whereis_name(name),
         {:ok, pid} <- Swarm.register_name(name, AmqpHiveClient.ConnectionManager, :register_consumer, [name, consumer, connection_name]) do
          # Logger.debug("[Registry] register_name #{inspect(pid)}")
          Swarm.join(@swarm_group, pid)
      {:ok, pid}
    else
      pid when is_pid(pid) ->
        # Logger.debug("[Registry] start_consumer exist #{inspect(pid)}")
        Swarm.join(@swarm_group, pid)
        {:ok, pid}

      {:error, {:already_registered, pid}} ->
        # Logger.debug("[Registry] start_consumer already exist #{inspect(pid)}")
        Swarm.join(@swarm_group, pid)
        {:ok, pid}

      {:error, som} = err ->
        Logger.info("[Registry] start_consumer error #{inspect(som)}")
        err
    end
  end

  def get_swarm_state(), do: :sys.get_state(Swarm.Tracker) #Swarm.members(@swarm_group)

  

  def resubscribe_queues() do
    Logger.info("Resubscribe to queues")
    connections = Application.get_env(:amqp_hive, :connections, [])
    Enum.reduce(connections, [], fn conn, acc ->
      conn_name = Map.get(conn, :name, "")
      connection = Map.get(conn, :connection, %{})
      
      consumers = Map.get(conn, :consumers, [])
      local_queue_names = 
        Enum.reduce(consumers, [], fn consumer, acc ->
          acc ++ [Map.get(consumer, :queue)]
        end)

      queues = get_queues(connection)
      Enum.each(queues, fn queue ->
        queue_name = Map.get(queue, "name")
        if !is_nil(queue_name) do
          case Enum.member?(local_queue_names, queue_name) do
            true -> 
              # Logger.info("Queue #{queue_name} is a member")
              nil
            false ->
              name = "#{conn_name}-#{queue_name}"

              consumer = %{
                name: name,
                queue: queue_name,
              }
              start_consumer_via_swarm(name, consumer, conn_name, "Initial Start")
          end
        end
      end)
    end)
  end

  def get_queues(%{host: host, virtual_host: vhost, username: username, password: password } = conn) do
    headers = [
      {:"Content-Type", "application/json"},
      {:Authorization, "Basic #{basic_auth_cred(username, password)}"}
    ]

    with {:ok, %HTTPoison.Response{body: body}} =
      HTTPoison.get(
        "#{rabbitmq_api_queue_url(host, vhost)}",
        headers,
        recv_timeout: 60_000
      ) do
      Poison.decode!(body)
    else
      error -> Logger.info("Error #{inspect(error)}")
      []
    end
  end

  def get_queues(_), do: []


  def rabbitmq_api_queue_url(host, virtual_host) do
    "https://#{host}/api/queues/#{virtual_host}"
  end

  def basic_auth_cred(username, password) do
    Base.encode64(username <> ":" <> password)
  end
end