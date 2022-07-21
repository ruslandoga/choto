defmodule ChotoTest do
  use ExUnit.Case

  test "connect and query and ping" do
    {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000)

    assert conn.revision == 54453
    assert conn.timezone in ["Europe/Moscow", "UTC"]

    # TODO {:ok, conn, req}? if clickhouse supports pipelining
    assert :ok = Choto.query(conn, "select 1 + 1")

    # TODO Choto.stream ?
    assert {:ok,
            [
              {:data, data1},
              {:data, data2},
              {:profile_info, profile_info1},
              {:progress, progress1},
              {:profile_events, profile_events},
              {:data, _data3 = []},
              {:progress, progress2},
              :end_of_stream
            ], conn} = Choto.await(conn)

    # From: https://github.com/ClickHouse/ClickHouse/blob/7722b647b75ff67c805b9d2f12208afae1056252/src/Core/Protocol.h#L51-L54:

    # If a query returns data, the server sends an empty header block containing
    # the description of resulting columns before executing the query.
    # Using this block the client can initialize the output formatter and display the prefix of resulting table
    # beforehand.

    assert data1 == [[{"plus(1, 1)", :u16}]]

    # TODO split columns from rows? %{columns: ["plus(1, 1)"], values: [[2]]}
    assert data2 == [[{"plus(1, 1)", :u16}, 2]]

    # TODO struct?
    assert profile_info1 == [
             _rows = 1,
             _blocks = 1,
             _bytes = 4104,
             _applied_limit = false,
             _rows_before_limit = 0,
             _calculated_rows_before_limit = true
           ]

    # TODO struct?
    assert progress1 == [
             _rows = 1,
             _bytes = 1,
             _total_rows = 0,
             _wrote_rows = 0,
             _wrote_bytes = 0
           ]

    profile_events = profile_events |> zip() |> load()
    assert value_for(profile_events, "SelectedRows") == 1
    assert value_for(profile_events, "SelectedBytes") == 1
    assert value_for(profile_events, "NetworkSendElapsedMicroseconds") > 1
    assert value_for(profile_events, "NetworkSendBytes") == 76

    assert progress2 == [
             _rows = 0,
             _bytes = 0,
             _total_rows = 0,
             _wrote_rows = 0,
             _wrote_bytes = 0
           ]

    assert conn.buffer == ""

    assert :ok = Choto.ping(conn)
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

  test "create table and insert data" do
    {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000)
    # :ok = Choto.query(conn, "drop table my_first_table")
    # assert {:ok, [_], conn} = Choto.await(conn)

    ddl = """
    create table my_first_table (
      user_id UInt32,
      message String,
      timestamp Datetime,
      metric Float32
    ) engine = MergeTree() primary key (user_id, timestamp)
    """

    assert :ok = Choto.query(conn, ddl)
    assert {:ok, [:end_of_stream], conn} = Choto.await(conn)

    on_exit(fn ->
      {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000)
      :ok = Choto.query(conn, "drop table my_first_table")
      {:ok, [:end_of_stream], _conn} = Choto.await(conn)
    end)

    assert :ok = Choto.query(conn, "select * from my_first_table")
    assert {:ok, packets, conn} = Choto.await(conn)

    assert Enum.filter(packets, fn packet -> match?({:data, _}, packet) end) ==
             [
               {:data,
                [
                  [{"user_id", :u32}],
                  [{"message", :string}],
                  [{"timestamp", :datetime}],
                  [{"metric", :f32}]
                ]},
               {:data, []}
             ]

    assert conn.buffer == ""

    :ok =
      Choto.query(conn, "INSERT INTO my_first_table (user_id, message, timestamp, metric) VALUES")

    today_date = Date.utc_today()
    yesterday_date = Date.add(today_date, -1)
    today = NaiveDateTime.new!(today_date, ~T[00:00:00])
    yesterday = NaiveDateTime.new!(yesterday_date, ~T[00:00:00])
    now = ~N[2022-07-21 14:24:37.799021]

    user_ids = [101, 102, 102, 101]

    messages = [
      "Hello, ClickHouse!",
      "Insert a lot of rows per batch",
      "Sort your data based on your commonly-used queries",
      "Granules are the smallest chunks of data read"
    ]

    timestamps = [now, yesterday, today, now]
    metrics = [-1.0, 1.41421, 2.718, 3.14159]

    :ok =
      Choto.send_data(conn, [
        ["user_id", :u32 | user_ids],
        ["message", :string | messages],
        ["timestamp", :datetime | timestamps],
        ["metric", :f32 | metrics]
      ])

    assert {:ok,
            [
              {:table_columns, table_columns},
              {:data, data},
              :end_of_stream
            ], conn} = Choto.await(conn)

    assert table_columns == [
             "columns format version: 1\n4 columns:\n`user_id` UInt32\n`message` String\n`timestamp` DateTime\n`metric` Float32\n"
           ]

    assert data == [
             [{"user_id", :u32}],
             [{"message", :string}],
             [{"timestamp", :datetime}],
             [{"metric", :f32}]
           ]

    assert conn.buffer == ""

    # TODO
    # assert :ok = Choto.query(conn, "select * from my_first_table")
    # assert {:ok, packets, conn} = Choto.await(conn)

    # assert Enum.filter(packets, fn packet -> match?({:data, _}, packet) end) ==
    #          [
    #            {:data,
    #             [
    #               [{"user_id", :u32}],
    #               [{"message", :string}],
    #               [{"timestamp", :datetime}],
    #               [{"metric", :f32}]
    #             ]},
    #            {:data, []}
    #          ]
  end
end
