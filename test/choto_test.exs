defmodule ChotoTest do
  use ExUnit.Case

  test "query" do
    # this is what clickhouse cli sends
    from_cli =
      "\x01$011b7efb-9127-40f1-87d5-b5404c19675c\x01\0$011b7efb-9127-40f1-87d5-b5404c19675c\t0.0.0.0:0\0\0\0\0\0\0\0\0\x01\x01q\nmac3.local\nClickHouse\x16\a\xB8\xA9\x03\0\0\x01\0\0\0\0\0\0\x02\0\rselect 1 + 1;\x02\0\x01\0\x02\xFF\xFF\xFF\xFF\0\0\0"

    assert {:ok,
            [
              type: _client_query = 1,
              query_id: "011b7efb-9127-40f1-87d5-b5404c19675c",
              query_kind: 1,
              initial_user: "",
              initial_query_id: "011b7efb-9127-40f1-87d5-b5404c19675c",
              initial_address: "0.0.0.0:0",
              timestamp: 0,
              interface: 1,
              os_user: "q",
              client_hostname: "mac3.local",
              client_name: "ClickHouse",
              version_major: 22,
              version_minor: 7,
              revision: 54456,
              quota_key: "",
              distributed_depth: 0,
              version_patch: 1,
              open_telemetry: 0,
              collaborate_with_initiator: 0,
              count_participating_replicas: 0,
              number_of_current_replica: 0,
              settings_end: "",
              interserver_secret: "",
              query_state: 2,
              compression: false,
              query: "select 1 + 1;"
            ],
            client_data} =
             decode_fields(from_cli,
               type: :varint,
               query_id: :string,
               query_kind: :u8,
               initial_user: :string,
               initial_query_id: :string,
               initial_address: :string,
               timestamp: :i64,
               interface: :u8,
               os_user: :string,
               client_hostname: :string,
               client_name: :string,
               version_major: :varint,
               version_minor: :varint,
               revision: :varint,
               quota_key: :string,
               distributed_depth: :varint,
               version_patch: :varint,
               open_telemetry: :u8,
               collaborate_with_initiator: :varint,
               count_participating_replicas: :varint,
               number_of_current_replica: :varint,
               settings_end: :string,
               interserver_secret: :string,
               query_state: :varint,
               compression: :boolean,
               query: :string
             )

    assert client_data == "\x02\0\x01\0\x02\xFF\xFF\xFF\xFF\0\0\0"

    assert decode_fields(client_data,
             type: :varint,
             #  TODO
             unknown: :varint,
             # block info
             block_info_field1: :varint,
             block_info_is_overflow: :u8,
             block_info_field2: :varint,
             block_info_bucket_num: :i32,
             block_info_num3: :varint,
             num_columns: :varint,
             num_rows: :varint
           ) ==
             {:ok,
              [
                type: _client_data = 2,
                unknown: 0,
                block_info_field1: 1,
                block_info_is_overflow: 0,
                block_info_field2: 2,
                block_info_bucket_num: -1,
                block_info_num3: 0,
                num_columns: 0,
                num_rows: 0
              ], ""}
  end

  # @tag skip: true
  test "connect" do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, 9000, active: false, mode: :binary)
    client_hello = Choto.Messages.client_hello("helloworld", "default", "")

    assert client_hello == [
             # client hello code
             <<0>>,
             # client name
             [<<5>> | "choto"],
             # version major
             "\x01",
             # version minor
             "\x01",
             # revision = 54453
             "\xB5\xA9\x03",
             # database
             ["\n" | "helloworld"],
             # username
             ["\a" | "default"],
             # password
             [<<0>> | ""]
           ]

    assert IO.iodata_to_binary(client_hello) ==
             "\0\x05choto\x01\x01\xB5\xA9\x03\nhelloworld\adefault\0"

    :ok = :gen_tcp.send(socket, client_hello)
    # 0 = server hello
    {:ok, <<0, data::bytes>>} = :gen_tcp.recv(socket, 0)

    assert data == "\nClickHouse\x16\a\xB8\xA9\x03\rEurope/Moscow\nmac3.local\x01"

    assert decode_fields(data,
             name: :string,
             version_major: :varint,
             version_minor: :varint,
             revision: :varint,
             timezone: :string,
             display_name: :string,
             version_patch: :varint
           ) ==
             {:ok,
              [
                name: "ClickHouse",
                version_major: 22,
                version_minor: 7,
                revision: 54456,
                timezone: "Europe/Moscow",
                display_name: "mac3.local",
                version_patch: 1
              ], <<>>}

    # TODO negotiate revision with server, revision = min(server, client)
    revision = min(_client = 54453, _server = 54456)

    client_query = Choto.Messages.client_query("select 1 + 1", revision)

    assert client_query == [
             <<1>>,
             [<<0>> | ""],
             [
               <<1>>,
               [<<0>> | ""],
               [<<0>> | ""],
               ["\t" | "0.0.0.0:0"],
               <<0, 0, 0, 0, 0, 0, 0, 0>>,
               <<1>>,
               [<<1>> | "q"],
               [<<4>> | "mac3"],
               [<<5>> | "choto"],
               <<1>>,
               <<1>>,
               "\xB5\xA9\x03",
               [<<0>> | ""],
               <<0>>,
               <<3>>,
               <<0>>,
               [<<0>>, <<0>>, <<0>>]
             ],
             [],
             [<<0>> | ""],
             [<<0>> | ""],
             <<2>>,
             <<0>>,
             ["\f" | "select 1 + 1"]
           ]

    assert IO.iodata_to_binary(client_query) ==
             "\x01\0\x01\0\0\t0.0.0.0:0\0\0\0\0\0\0\0\0\x01\x01q\x04mac3\x05choto\x01\x01\xB5\xA9\x03\0\0\x03\0\0\0\0\0\0\x02\0\fselect 1 + 1"

    :ok = :gen_tcp.send(socket, client_query)

    client_data = Choto.Messages.client_data()

    # TODO can ints < u8 be just ints, not bins? does it affect performance?
    assert client_data == [
             <<2>>,
             0,
             [[<<1>>, <<0>>, <<2>>, <<255, 255, 255, 255>>, <<0>>], 0, 0]
           ]

    assert IO.iodata_to_binary(client_data) == "\x02\0\x01\0\x02\xFF\xFF\xFF\xFF\0\0\0"
    :ok = :gen_tcp.send(socket, client_data)

    # 1 = server data

    # https://github.com/ClickHouse/ClickHouse/blob/7722b647b75ff67c805b9d2f12208afae1056252/src/Core/Protocol.h#L51-L54

    # If a query returns data, the server sends an empty header block containing
    # the description of resulting columns before executing the query.
    # Using this block the client can initialize the output formatter and display the prefix of resulting table
    # beforehand.
    {:ok, <<1, data::bytes>>} = :gen_tcp.recv(socket, 0)
    assert data == "\0\x01\0\x02\xFF\xFF\xFF\xFF\0\x01\0\nplus(1, 1)\x06UInt16"

    assert {:ok,
            [
              unknown: 0,
              block_info_field1: 1,
              block_info_is_overflow: 0,
              block_info_field2: 2,
              block_info_bucket_num: -1,
              block_info_num3: 0,
              num_columns: 1,
              num_rows: 0
            ],
            columns} =
             decode_fields(data,
               unknown: :varint,
               # block info
               block_info_field1: :varint,
               block_info_is_overflow: :u8,
               block_info_field2: :varint,
               block_info_bucket_num: :i32,
               block_info_num3: :varint,
               # block content
               num_columns: :varint,
               num_rows: :varint
             )

    assert columns == "\nplus(1, 1)\x06UInt16"

    assert decode_fields(columns, name: :string, type: :string) ==
             {:ok, [name: "plus(1, 1)", type: "UInt16"], ""}

    # TODO just receive until exception or EOS, don't hardcode packet sizes

    {:ok, <<1, data::bytes>>} = :gen_tcp.recv(socket, byte_size(data) + 3)
    assert data == "\0\x01\0\x02\xFF\xFF\xFF\xFF\0\x01\x01\nplus(1, 1)\x06UInt16\x02\0"

    assert {:ok,
            [
              unknown: 0,
              block_info_field1: 1,
              block_info_is_overflow: 0,
              block_info_field2: 2,
              block_info_bucket_num: -1,
              block_info_num3: 0,
              num_columns: 1,
              num_rows: 1
            ],
            content} =
             decode_fields(data,
               unknown: :varint,
               # block info
               block_info_field1: :varint,
               block_info_is_overflow: :u8,
               block_info_field2: :varint,
               block_info_bucket_num: :i32,
               block_info_num3: :varint,
               # block content
               num_columns: :varint,
               num_rows: :varint
             )

    assert content == "\nplus(1, 1)\x06UInt16\x02\0"

    assert {:ok, [name: "plus(1, 1)", type: "UInt16"], rows} =
             decode_fields(content, name: :string, type: :string)

    assert rows == "\x02\0"

    assert decode_fields(rows, plus: :u16) == {:ok, [plus: 2], ""}

    # 6 = profile info
    {:ok, <<6, data::bytes>>} = :gen_tcp.recv(socket, 8)
    assert data == "\x01\x01\x88 \0\0\x01"

    # 3 = progress
    {:ok, <<3, data::bytes>>} = :gen_tcp.recv(socket, 6)
    assert data == "\x01\x01\0\0\0"

    # 15 = profile events
    {:ok, <<14, _data::bytes>>} = :gen_tcp.recv(socket, 764)

    # assert data ==
    #          "\0\x01\0\x02\xFF\xFF\xFF\xFF\0\x06\r\thost_name\x06String\0\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\nmac3.local\fcurrent_time\bDateTime\0\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\x16\x8A\xD4b\tthread_id\x06UInt64\0۽V\0\0\0\0\0۽V\0\0\0\0\0۽V\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x04type#Enum8('increment' = 1, 'gauge' = 2)\0\x01\x01\x02\x01\x01\x01\x01\x01\x01\x01\x01\x01\x02\x04name\x06String\0\fSelectedRows\rSelectedBytes\x12MemoryTrackerUsage\x05Query\vSelectQuery\x1ENetworkSendElapsedMicroseconds\x10NetworkSendBytes\fSelectedRows\rSelectedBytes\vContextLock\x17RWLockAcquiredReadLocks\x14RealTimeMicroseconds\x12MemoryTrackerUsage\x05value\x05Int64\0\x01\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\xE0!\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0>\0\0\0\0\0\0\0N\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\n\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x1C\x01\0\0\0\0\0\0\xE0!\0\0\0\0\0\0"

    {:ok, <<1, data::bytes>>} = :gen_tcp.recv(socket, 12)
    assert data == "\0\x01\0\x02\xFF\xFF\xFF\xFF\0\0\0"

    # 3 = progress
    {:ok, <<3, data::bytes>>} = :gen_tcp.recv(socket, 6)
    assert data == "\0\0\0\0\0"

    # 5 = end of stream
    {:ok, <<5>>} = :gen_tcp.recv(socket, 0)

    on_exit(fn -> :gen_tcp.close(socket) end)
  end

  def decode_fields(bytes, fields) do
    alias Choto.Decoder
    types = Enum.map(fields, fn {_key, type} -> type end)

    case Decoder.decode(bytes, types) do
      {:ok, rest, values} ->
        keys = Enum.map(fields, fn {key, _type} -> key end)
        {:ok, Enum.zip(keys, values), rest}

      other ->
        other
    end
  end
end

# 2 = server exception
# {:ok, <<2, exception::bytes>>} ->
#   assert decode_fields(
#            exception,
#            :struct,
#            code: :i32,
#            name: :string,
#            message: :string,
#            stack_trace: :string,
#            has_nested: :boolean
#          ) ==
#            {:ok,
#             [
#               code: 62,
#               name: "DB::Exception",
#               message:
#                 "DB::Exception: Syntax error: failed at position 12 (end of query): . Expected one of: INTERVAL operator expression, INTERVAL, TIMESTAMP operator expression, TIMESTAMP, DATE operator expression, DATE, multiplicative expression, list, delimited by binary operators, unary expression, expression with prefix unary operator, NOT, CAST expression, tuple element expression, array element expression, element of expression, SELECT subquery, CAST operator, tuple, collection of literals, parenthesized expression, array, literal, NULL, number, Bool, true, false, string literal, case, CASE, COLUMNS matcher, COLUMNS, function, identifier, qualified asterisk, compound identifier, list of elements, asterisk, substitution, MySQL-style global variable",
#               stack_trace: "<Empty trace>\n",
#               has_nested: false
#             ], ""}
