defmodule Choto.ClickHouseError do
  @moduledoc """
  A ClickHouse error.

  Contains the following fields:

    - `:code` - [ClickHouse error code](https://clickhouse.com/codebrowser/ClickHouse/src/Common/ErrorCodes.cpp.html)
    - `:message` - message received from the server

  """

  @type t :: %__MODULE__{code: pos_integer, message: iodata}
  defexception [:code, :message]

  def message(%__MODULE__{message: message}) do
    IO.iodata_to_binary(message)
  end
end
