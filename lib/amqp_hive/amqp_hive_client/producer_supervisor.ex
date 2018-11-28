defmodule AmqpHiveClient.ProducerSupervisor do
  use DynamicSupervisor

  require Logger

  def start_link(name) do
    DynamicSupervisor.start_link(__MODULE__, {name}, name: name)
  end

  def child_spec(connection_name) do
    name = get_name(connection_name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [name]}
    }
  end

  def start_producer(producer, connection_name) do
    Logger.info(fn ->
      "Start Producer from Supervisor #{inspect(producer)} and parent = #{
        inspect(connection_name)
      }"
    end)

    name = get_name(connection_name)

    DynamicSupervisor.start_child(
      name,
      AmqpHiveClient.Producer.child_spec(producer, connection_name)
    )
  end

  def stop_producer(connection_name, name) when is_atom(name) do
    producer_name = get_name(connection_name)

    case Process.whereis(name) do
      nil ->
        {:error, "No process"}

      pid ->
        DynamicSupervisor.terminate_child(
          producer_name,
          pid
        )
    end
  end

  def stop_producer(connection_name, pid) do
    producer_name = get_name(connection_name)

    DynamicSupervisor.terminate_child(
      producer_name,
      pid
    )
  end

  def init({_name}) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def get_name(connection_name), do: :"#{connection_name}-producer"


  def get_available_producer(connection_name) do
    supname = get_name(connection_name)
    children = DynamicSupervisor.which_children(supname)
    
    {bestpid, cnt} =
      Enum.reduce(children, {nil, -1}, fn {_, pid, _typ, _module}, {bestpid, cnt} ->
        process_info = Process.info(pid)        
        message_len = process_info[:message_queue_len]
        if message_len < cnt or is_nil(bestpid)  do
          {pid, message_len}
        else
          {bestpid, cnt}
        end
      end)
      if cnt > 0 do
        Logger.info("Producer Process Cnt= #{cnt}")
      end
      # Logger.info("Producer Supervisor Get_available_producer Info = #{inspect(bestpid)} with cnt of #{inspect(cnt)}")
      {:ok, bestpid}
  end
  # Nice utility method to check which processes are under supervision
  def children do
    Supervisor.which_children(__MODULE__)
  end

  # Nice utility method to check which processes are under supervision
  def count_children do
    Supervisor.count_children(__MODULE__)
  end
end
