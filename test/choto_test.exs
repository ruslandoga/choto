defmodule ChotoTest do
  use ExUnit.Case
  doctest Choto

  test "greets the world" do
    assert Choto.hello() == :world
  end
end
