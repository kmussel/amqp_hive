defmodule AmqpHive.RpcRegistry do
  use GenServer
  require Logger

  @name __MODULE__
  @swarm_group :amqp_hive_tasks


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

  def register_task(name, task_args) do
    GenServer.call(@name, {:register_task, name, task_args})
  end

  def monitor(pid, {task_name, task_args}) do
    GenServer.cast(@name, {:monitor, pid, {task_name, task_args}})
  end

  def log_state() do
    send(Process.whereis(@name), :log_state)
  end

  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, %{}}
  end

  def handle_cast({:monitor, pid, task_args}, state) do
    ref = Process.monitor(pid)
    {:noreply, Map.put(state, ref, task_args)}
  end

  def handle_call({:register_task, name, task_args}, _from, state) do
    case start_consumer_via_swarm(name, task_args) do
      {:ok, pid} ->        
        {:reply, {:ok, pid}, state}
      {:already_registered, pid} ->
        {:reply, {:ok, pid}, state}
      {:error, reason} ->
        Logger.error("[Registry] error starting #{name} - #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end


  def handle_info(:log_state, state) do
    total = Swarm.registered() |> Enum.count()
    local = state |>  Enum.count()

    Logger.debug("[Task Registry] Totals: #{inspect(get_swarm_state())}  Swarm/#{total} Local/#{local}")
    Process.send_after(self(), :log_state, 15000)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
    # Logger.debug("[Registry] DOWN NORMAL #{inspect(Map.get(state, ref))}")
    case Map.get(state, ref) do
      {name, _task_args} ->
        Swarm.unregister_name(name)
      _other -> nil
    end
    {:noreply, Map.delete(state, ref)}
  end

  def handle_info({:DOWN, ref, :process, _pid, :shutdown}, state) do
    case Map.get(state, ref) do
      {name, _task_args} ->
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
      {name, task_args} ->
        Swarm.unregister_name(name)
        {:ok, _pid} = start_consumer_via_swarm(name, task_args, "restarting")
        {:noreply, Map.delete(state, ref)}
      other -> 
        Logger.debug("[Registry] :DOWN #{inspect(other)}")
        {:noreply, Map.delete(state, ref)}
    end
  end

  def start_consumer_via_swarm(name, task_args, _reason \\ "starting") do
    with :undefined <- Swarm.whereis_name(name),
         {:ok, pid} <- Swarm.register_name(name, AmqpHiveClient.Rpc, :register_task, [name, task_args]) do
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
