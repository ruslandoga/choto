defmodule Choto do
  @moduledoc false
  alias Choto.{Messages, Decoder}

  #######################
  # server packet types #
  #######################

  # Name, version, revision.
  @server_hello 0

  # A block of data (compressed or not).
  @server_data 1

  # The exception during query execution.
  @server_exception 2

  # Query execution progress: rows read, bytes read.
  @server_progress 3

  # Ping response
  # @server_pong 4

  # All packets were transmitted
  @server_end_of_stream 5

  # Packet with profiling info.
  @server_profile_info 6

  # A block with totals (compressed or not).
  # @server_totals 7

  # A block with minimums and maximums (compressed or not).
  # @server_extremes 8

  # A response to TablesStatus request.
  # @server_table_status_response 9

  # System logs of the query execution
  # @server_log 10

  # Columns' description for default values calculation
  # @server_table_columns 11

  # List of unique parts ids.
  # @server_part_uuids 12

  # String (UUID) describes a request for which next task is needed
  # This is such an inverted logic, where server sends requests
  # And client returns back response
  # @server_read_task_request 13

  # Packet with profile events from server.
  @server_profile_events 14

  # Request from a MergeTree replica to a coordinator
  # @server_merge_tree_read_task_request 15

  @type conn :: %{
          socket: :gen_tcp.socket(),
          revision: pos_integer(),
          timezone: String.t(),
          buffer: binary
        }

  # TODO handle errors
  @spec connect(:inet.hostname() | :inet.ip_address(), :inet.port_number(), Keyword.t()) ::
          {:ok, conn}
  def connect(host, port, opts) do
    {:ok, socket} = :gen_tcp.connect(host, port, active: false, mode: :binary)
    database = opts[:database] || "default"
    username = opts[:username] || "default"
    password = opts[:password] || ""
    client_hello = Messages.client_hello(database, username, password)
    :ok = :gen_tcp.send(socket, client_hello)
    {:ok, <<@server_hello, hello::bytes>>} = :gen_tcp.recv(socket, 0)

    {:ok, "",
     [_name, _version_major, _version_minor, revision, timezone, _display_name, _version_patch]} =
      Decoder.decode(hello, [:string, :varint, :varint, :varint, :string, :string, :varint])

    {:ok,
     %{
       socket: socket,
       revision: min(Messages.revision(), revision),
       timezone: timezone,
       buffer: ""
     }}
  end

  # TODO await resp? record query id for Choto.stream?
  @spec query(conn, String.t()) :: {:ok, conn}
  def query(conn, query) do
    %{socket: socket, revision: revision} = conn
    :ok = :gen_tcp.send(socket, Messages.client_query(query, revision))
    :ok = :gen_tcp.send(socket, Messages.client_data())
    {:ok, conn}
  end

  # TODO
  @type server_packet :: term

  # TODO handle errors
  @spec await(conn) :: {conn, [server_packet]}
  def await(conn, acc \\ []) do
    # TODO use revision in decode
    %{socket: socket, revision: _revision, buffer: buffer} = conn
    {:ok, data} = :gen_tcp.recv(socket, 0)

    case decode(buffer <> data, acc) do
      {:cont, buffer, acc} -> await(%{conn | buffer: buffer}, acc)
      {:done, buffer, acc} -> {%{conn | buffer: buffer}, acc}
    end
  end

  @spec close(conn) :: :ok
  def close(%{socket: socket}) do
    :gen_tcp.close(socket)
  end

  # TODO in decode, handle incomplete blocks / messages
  # not just {:ok, rest, decoded} = ...

  @doc false
  # TODO what is this 0?
  def decode(<<@server_data, 0, rest::bytes>>, acc) do
    {:ok, rest, block} = Decoder.decode_block(rest)
    decode(rest, [{:data, block} | acc])
  end

  def decode(<<@server_profile_info, rest::bytes>>, acc) do
    {:ok, rest, profile_info} =
      Decoder.decode(rest, [
        _rows = :varint,
        _blocks = :varint,
        _bytes = :varint,
        _applied_limit = :boolean,
        _rows_before_limit = :varint,
        _calculated_rows_before_limit = :boolean
      ])

    decode(rest, [{:profile_info, profile_info} | acc])
  end

  def decode(<<@server_progress, rest::bytes>>, acc) do
    {:ok, rest, progress} =
      Decoder.decode(
        rest,
        [
          _rows = :varint,
          _bytes = :varint,
          _total_rows = :varint,
          # TODO if revision > DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
          _wrote_rows = :varint,
          _wrote_bytes = :varint
        ]
      )

    decode(rest, [{:progress, progress} | acc])
  end

  def decode(<<@server_profile_events, 0, rest::bytes>>, acc) do
    {:ok, rest, profile_events} = Decoder.decode_block(rest)
    decode(rest, [{:profile_events, profile_events} | acc])
  end

  def decode(<<@server_end_of_stream, rest::bytes>>, acc) do
    {:done, rest, :lists.reverse([:end_of_stream | acc])}
  end

  def decode(<<@server_exception, rest::bytes>>, acc) do
    {:ok, rest, exception} =
      Decoder.decode(rest, [
        _code = :i32,
        _name = :string,
        _message = :string,
        _stack_trace = :string,
        _has_nested = :boolean
      ])

    {:done, rest, :lists.reverse([{:exception, exception} | acc])}
  end

  def decode(bytes, acc) do
    {:cont, bytes, acc}
  end
end
