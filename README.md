### Choto

Native [ClickHouse](https://github.com/ClickHouse/ClickHouse) protocol + client, API modelled after [`:mint.`](https://github.com/elixir-mint/mint)

```elixir
{:ok, conn} = Choto.connect(hostname, port, _options = [])
{:ok, conn, ref} = Choto.query(conn, "select 1 + 1")

receive do
  message -> Choto.stream(conn, message)
  # {:server_data, _block = [{_column_name, _column_type} | values]} | _rest]}
  # {:server_profile_info, ...}
  # {:server_progress, _rows, _bytes, _total_rows, _wrote_rows, _wrote_bytes}
  # {:server_profile_events, _block = [...]}
  # :server_end_of_stream
end
```

[`:choto_dbconnection`]()

[`:choto_ecto`]()
