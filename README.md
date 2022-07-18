### TODO

Native [ClickHouse](https://github.com/ClickHouse/ClickHouse) protocol + client, API modelled after [`:mint.`](https://github.com/elixir-mint/mint)

```elixir
{:ok, conn} = Choto.connect(hostname, port, _options = [])
{:ok, conn, ref} = Choto.query(conn, "select 1 + 1")

receive do
  message -> Choto.stream(conn, message)
end
```

[`:choto_dbconnection`]()

[`:choto_ecto`]()
