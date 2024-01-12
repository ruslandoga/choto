# Choto

[Native ClickHouse](https://clickhouse.com/docs/en/native-protocol) client.

```elixir
iex> {:ok, conn} = Choto.connect(:tcp, _hostname = {127, 0, 0, 1}, _port = 9000)
iex> :ok = Choto.send(conn, Choto.client_query("select 1 + 1"))
iex> {:ok, responses} = Choto.recv(conn, ref, _timeout = :timer.seconds(5))
```
