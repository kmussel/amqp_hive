defmodule AmqpHiveTest do
  use ExUnit.Case
  doctest AmqpHive

  test "greets the world" do
    assert AmqpHive.hello() == :world
  end
end
