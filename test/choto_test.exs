defmodule ChotoTest do
  use ExUnit.Case

  test "connect and query and ping" do
    {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000)

    assert conn.revision == 54453
    assert conn.timezone in ["Europe/Moscow", "UTC"]

    # TODO {:ok, conn, req}? if clickhouse supports pipelining
    assert {:ok, conn} = Choto.query(conn, "select 1 + 1")

    # TODO Choto.stream ?
    assert {:ok,
            [
              # From: https://github.com/ClickHouse/ClickHouse/blob/7722b647b75ff67c805b9d2f12208afae1056252/src/Core/Protocol.h#L51-L54:
              # If a query returns data, the server sends an empty header block containing
              # the description of resulting columns before executing the query.
              # Using this block the client can initialize the output formatter and display the prefix of resulting table
              # beforehand.
              {:data, [[{"plus(1, 1)", :u16}]]},
              # TODO split columns from rows? %{columns: ["plus(1, 1)"], values: [[2]]}
              {:data, [[{"plus(1, 1)", :u16}, 2]]},
              # TODO struct?
              {:profile_info,
               [
                 _rows0 = 1,
                 _blocks0 = 1,
                 _bytes0 = 4104,
                 _applied_limit0 = false,
                 _rows_before_limit0 = 0,
                 _calculated_rows_before_limit0 = true
               ]},
              # TODO struct?
              {:progress,
               [_rows1 = 1, _bytes1 = 1, _total_rows1 = 0, _wrote_rows1 = 0, _wrote_bytes1 = 0]},
              {:profile_events, events},
              {:data, []},
              {:progress,
               [_rows2 = 0, _bytes2 = 0, _total_rows2 = 0, _wrote_rows2 = 0, _wrote_bytes2 = 0]},
              :end_of_stream
            ], conn} = Choto.await(conn)

    events = events |> zip() |> load()
    assert value_for(events, "SelectedRows") == 1
    assert value_for(events, "SelectedBytes") == 1
    assert value_for(events, "NetworkSendElapsedMicroseconds") > 1
    assert value_for(events, "NetworkSendBytes") == 76

    assert conn.buffer == ""

    assert {:ok, conn} = Choto.ping(conn)
    assert {:ok, :pong, conn} = Choto.recv(conn)

    assert conn.buffer == ""
  end

  defp zip(block) do
    [header | rows] = Enum.zip(block)
    header = header |> Tuple.to_list() |> Enum.map(fn {name, _type} -> name end)
    rows = Enum.map(rows, &Tuple.to_list/1)
    %{columns: header, rows: rows}
  end

  defp load(%{columns: columns, rows: rows}) do
    Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
  end

  defp value_for(events, name) do
    events
    |> Enum.find(fn %{"name" => event_name} -> event_name == name end)
    |> Map.fetch!("value")
  end
end
