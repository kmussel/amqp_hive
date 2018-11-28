defmodule AmqpHiveClient.ConsumerSupervisor do
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

  def start_consumer(consumer, connection_name) do
    # Logger.info(fn ->
    #   "Start Consumer from Supervisor #{inspect(consumer)} and parent = #{
    #     inspect(connection_name)
    #   }"
    # end)

    name = get_name(connection_name)

    DynamicSupervisor.start_child(
      name,
      AmqpHiveClient.Consumer.child_spec(consumer, connection_name)
    )
  end

  def stop_consumer(connection_name, name) when is_atom(name) do
    consumer_name = get_name(connection_name)

    case Process.whereis(name) do
      nil ->
        {:error, "No process"}

      pid ->
        DynamicSupervisor.terminate_child(
          consumer_name,
          pid
        )
    end
  end

  def stop_consumer(connection_name, pid) do
    consumer_name = get_name(connection_name)

    DynamicSupervisor.terminate_child(
      consumer_name,
      pid
    )
  end

  def init({_name}) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def get_name(connection_name), do: :"#{connection_name}-consumer"
  
  def get_available_consumer(connection_name) do
    supname = get_name(connection_name)
    children = DynamicSupervisor.which_children(supname)
    
    {bestpid, cnt} =
      Enum.reduce(children, {nil, 0}, fn {_, pid, _typ, _module}, {bestpid, cnt} ->
        process_info = Process.info(pid)
        # Logger.info("Process Info = #{inspect(process_info)}")

        message_len = process_info[:message_queue_len]
        if message_len < cnt or is_nil(bestpid)  do
          {pid, message_len}
        else
          {bestpid, cnt}
        end
        
      end)
      Logger.info("Producer Supervisor Get_available_producer Info = #{inspect(bestpid)} with cnt of #{inspect(cnt)}")
      {:ok, bestpid}
  end

  # Nice utility method to check which processes are under supervision
  def children(connection_name) do
    supname = get_name(connection_name)
    DynamicSupervisor.which_children(supname)
  end

  # Nice utility method to check which processes are under supervision
  def count_children(connection_name) do
    supname = get_name(connection_name)
    DynamicSupervisor.count_children(supname)
  end
end


