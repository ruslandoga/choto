defmodule ChotoTest do
  use ExUnit.Case

  test "connect and query" do
    {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000, table: "helloworld")

    assert conn.revision == 54453
    assert conn.timezone == "Europe/Moscow"

    # TODO {:ok, conn, req}? if clickhouse supports pipelining
    assert {:ok, conn} = Choto.query(conn, "select 1 + 1")

    # TODO Choto.stream ?
    assert {conn,
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
              {:profile_events,
               [
                 [
                   {"host_name", :string},
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local",
                   "mac3.local"
                 ],
                 [
                   {"current_time", :datetime},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{},
                   %NaiveDateTime{}
                 ],
                 [{"thread_id", :u64}, _, _, _, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                 [{"type", :u8}, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
                 [
                   {"name", :string},
                   "SelectedRows",
                   "SelectedBytes",
                   "MemoryTrackerUsage",
                   "Query",
                   "SelectQuery",
                   "NetworkSendElapsedMicroseconds",
                   "NetworkSendBytes",
                   "SelectedRows",
                   "SelectedBytes",
                   "ContextLock",
                   "RWLockAcquiredReadLocks",
                   "RealTimeMicroseconds",
                   "MemoryTrackerUsage"
                 ],
                 [{"value", :i64}, 1, 1, 8672, 1, 1, _, 76, 1, 1, 10, 1, _, 8672]
               ]},
              {:data, []},
              {:progress,
               [_rows2 = 0, _bytes2 = 0, _total_rows2 = 0, _wrote_rows2 = 0, _wrote_bytes2 = 0]},
              :end_of_stream
            ]} = Choto.await(conn)

    assert conn.buffer == ""
  end
end
