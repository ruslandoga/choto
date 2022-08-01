### Choto

Native [ClickHouse](https://github.com/ClickHouse/ClickHouse) protocol + client, API (to be) modelled after [`:mint.`](https://github.com/elixir-mint/mint)

```elixir
iex> {:ok, conn} = Choto.connect(_hostname = {127, 0, 0, 1}, _port = 9000, _options = [])
iex> {:ok, conn} = Choto.query(conn, "select 1 + 1")

# See tests for more
iex> {:ok, _decoded_packets, conn} = Choto.await(conn)
```

[`:choto_dbconnection`](https://github.com/ruslandoga/choto_dbconnection)

[`:choto_ecto`]()
