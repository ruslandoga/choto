# Choto

Native [ClickHouse](https://github.com/ClickHouse/ClickHouse) client.

```elixir
iex> {:ok, conn} = Choto.connect(:tcp, _hostname = {127, 0, 0, 1}, _port = 9000)
iex> {:ok, ref, conn} = Choto.send(conn, "select 1 + 1")
iex> {:ok, responses} = Choto.recv(conn, ref, _timeout = :timer.seconds(5))
```
