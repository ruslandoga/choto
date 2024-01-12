defmodule Choto do
  @moduledoc """
  TODO
  """

  @dialyzer :no_improper_lists

  import Kernel, except: [send: 2]
  import Bitwise

  @hello 0
  @query 1

  def connect(:tcp, addr, port, opts \\ []) do
    transport_opts = Keyword.get(opts, :transport_opts, [])
    transport_opts = [active: false, mode: :binary] ++ transport_opts
    timeout = Keyword.get(opts, :timeout, :timer.seconds(5))

    with {:ok, socket} <- :gen_tcp.connect(addr, port, transport_opts) do
      with :ok <- send(socket, client_hello()) do
        {:ok, <<@hello, hello::bytes>>} = recv(socket, 0, timeout)

        [name, version_major, version_minor, revision, timezone, display_name, version_patch] =
          decode(hello, [:string, :varint, :varint, :varint, :string, :string, :varint])

        {:ok, socket,
         %{
           name: name,
           version_major: version_major,
           version_minor: version_minor,
           revision: revision,
           timezone: timezone,
           display_name: display_name,
           version_patch: version_patch
         }}
      end
    end
  end

  def send(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)

  def recv(socket, length, timeout) when is_port(socket) do
    :gen_tcp.recv(socket, length, timeout)
  end

  # https://clickhouse.com/docs/en/native-protocol/client#hello
  def client_hello(
        client_name \\ "Choto",
        version_major \\ 0,
        version_minor \\ 1,
        protocol_version \\ 54451,
        database \\ "default",
        username \\ "default",
        password \\ ""
      ) do
    [
      @hello,
      encode(:string, client_name),
      encode(:varint, version_major),
      encode(:varint, version_minor),
      encode(:varint, protocol_version),
      encode(:string, database),
      encode(:string, username),
      encode(:string, password)
    ]
  end

  # https://clickhouse.com/docs/en/native-protocol/client#query
  def client_query(
        id \\ "",
        client_info \\ client_info(),
        settings \\ client_settings([]),
        secret \\ "",
        stage \\ 2,
        compression \\ 0,
        body
      ) do
    [
      @query,
      encode(:string, id),
      client_info,
      settings,
      encode(:string, secret),
      encode(:varint, stage),
      encode(:varint, compression)
      | encode(:string, body)
    ]
  end

  # https://clickhouse.com/docs/en/native-protocol/client#client-info
  def client_info(
        query_kind \\ 1,
        initial_user \\ "",
        initial_query_id \\ "",
        initial_address \\ "",
        initial_time \\ 0,
        interface \\ 1,
        os_user \\ "",
        client_hostname \\ "",
        client_name \\ "",
        version_major \\ 0,
        version_minor \\ 1,
        protocol_version \\ 0,
        quota_key \\ "",
        distributed_depth \\ 0,
        version_patch \\ 0,
        otel \\ 0,
        trace_id \\ "",
        span_id \\ "",
        trace_state \\ "",
        trace_flags \\ 0
      ) do
    [
      query_kind,
      encode(:string, initial_user),
      encode(:string, initial_query_id),
      encode(:string, initial_address),
      encode(:i64, initial_time),
      interface,
      encode(:string, os_user),
      encode(:string, client_hostname),
      encode(:string, client_name),
      encode(:varint, version_major),
      encode(:varint, version_minor),
      encode(:varint, protocol_version),
      encode(:string, quota_key),
      encode(:varint, distributed_depth),
      encode(:varint, version_patch),
      otel,
      encode({:fixed_string, 16}, trace_id),
      encode({:fixed_string, 8}, span_id),
      encode(:string, trace_state),
      trace_flags
    ]
  end

  def client_settings([{key, value} | rest]) do
    [encode(:string, key), encode(:string, value) | client_settings(rest)]
  end

  def client_settings([]) do
    # blank key and value denotes end of list
    [_key = encode(:string, ""), _value = encode(:string, "")]
  end

  def encode(:varint, i) when is_integer(i) and i < 128, do: i
  def encode(:varint, i) when is_integer(i), do: encode_varint_cont(i)

  def encode(:string, s) do
    case s do
      _ when is_binary(s) -> [encode(:varint, byte_size(s)) | s]
      _ when is_list(s) -> [encode(:varint, IO.iodata_length(s)) | s]
      nil -> 0
    end
  end

  def encode({:fixed_string, size}, s) when byte_size(s) == size do
    s
  end

  def encode({:fixed_string, size}, s) when byte_size(s) < size do
    to_pad = size - byte_size(s)
    [s | <<0::size(to_pad * 8)>>]
  end

  def encode({:fixed_string, size}, nil), do: <<0::size(size * 8)>>

  def encode(:u8, u) when is_integer(u), do: u
  def encode(:u8, nil), do: 0

  def encode(:i8, i) when is_integer(i) and i >= 0, do: i
  def encode(:i8, i) when is_integer(i), do: <<i::signed>>
  def encode(:i8, nil), do: 0

  for size <- [16, 32, 64, 128, 256] do
    def encode(unquote(:"u#{size}"), u) when is_integer(u) do
      <<u::unquote(size)-little>>
    end

    def encode(unquote(:"i#{size}"), i) when is_integer(i) do
      <<i::unquote(size)-little-signed>>
    end

    def encode(unquote(:"u#{size}"), nil), do: <<0::unquote(size)>>
    def encode(unquote(:"i#{size}"), nil), do: <<0::unquote(size)>>
  end

  def decode(data, types) do
    decode_cont(types, data, [])
  end

  defp encode_varint_cont(i) when i < 128, do: <<i>>

  defp encode_varint_cont(i) do
    [(i &&& 0b0111_1111) ||| 0b1000_0000 | encode_varint_cont(i >>> 7)]
  end

  varints = [
    {_pattern = quote(do: <<0::1, v1::7>>), _value = quote(do: v1)},
    {quote(do: <<1::1, v1::7, 0::1, v2::7>>), quote(do: (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 0::1, v3::7>>),
     quote(do: (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 0::1, v4::7>>),
     quote(do: (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 0::1, v5::7>>),
     quote(do: (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 0::1, v6::7>>),
     quote(do: (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote do
       <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 0::1,
         v7::7>>
     end,
     quote do
       (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1
     end},
    {quote do
       <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 1::1,
         v7::7, 0::1, v8::7>>
     end,
     quote do
       (v8 <<< 49) + (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) +
         (v2 <<< 7) + v1
     end}
  ]

  for {pattern, size} <- varints do
    defp decode_string_cont(
           <<unquote(pattern), s::size(unquote(size))-bytes, rest::bytes>>,
           types,
           acc
         ) do
      decode_cont(types, rest, [s | acc])
    end
  end

  for {pattern, size} <- varints do
    defp decode_varint_cont(<<unquote(pattern), rest::bytes>>, types, acc) do
      decode_cont(types, rest, [unquote(size) | acc])
    end
  end

  defp decode_cont([type | types], <<bin::bytes>>, acc) do
    case type do
      :string -> decode_string_cont(bin, types, acc)
      :varint -> decode_varint_cont(bin, types, acc)
    end
  end

  defp decode_cont([], <<>>, acc) do
    :lists.reverse(acc)
  end

  # @moduledoc false
  # alias Choto.{Messages, Decoder}

  # #######################
  # # server packet types #
  # #######################

  # # Name, version, revision.
  # @server_hello 0

  # # A block of data (compressed or not).
  # @server_data 1

  # # The exception during query execution.
  # @server_exception 2

  # # Query execution progress: rows read, bytes read.
  # @server_progress 3

  # # Ping response
  # @server_pong 4

  # # All packets were transmitted
  # @server_end_of_stream 5

  # # Packet with profiling info.
  # @server_profile_info 6

  # # A block with totals (compressed or not).
  # # @server_totals 7

  # # A block with minimums and maximums (compressed or not).
  # # @server_extremes 8

  # # A response to TablesStatus request.
  # # @server_table_status_response 9

  # # System logs of the query execution
  # # @server_log 10

  # # Columns' description for default values calculation
  # @server_table_columns 11

  # # List of unique parts ids.
  # # @server_part_uuids 12

  # # String (UUID) describes a request for which next task is needed
  # # This is such an inverted logic, where server sends requests
  # # And client returns back response
  # # @server_read_task_request 13

  # # Packet with profile events from server.
  # @server_profile_events 14

  # # Request from a MergeTree replica to a coordinator
  # # @server_merge_tree_read_task_request 15

  # @type conn :: %{
  #         socket: :gen_tcp.socket(),
  #         revision: pos_integer(),
  #         timezone: String.t(),
  #         buffer: binary
  #       }

  # # TODO handle errors
  # @spec connect(:inet.hostname() | :inet.ip_address(), :inet.port_number(), Keyword.t()) ::
  #         {:ok, conn}
  # def connect(host, port, opts \\ []) do
  #   {:ok, socket} = :gen_tcp.connect(host, port, active: false, mode: :binary)
  #   database = opts[:database] || "default"
  #   username = opts[:username] || "default"
  #   password = opts[:password] || ""
  #   client_hello = Messages.client_hello(database, username, password)
  #   :ok = :gen_tcp.send(socket, client_hello)
  #   {:ok, <<@server_hello, hello::bytes>>} = :gen_tcp.recv(socket, 0)

  #   {:ok, "",
  #    [_name, _version_major, _version_minor, revision, timezone, _display_name, _version_patch]} =
  #     Decoder.decode(hello, [:string, :varint, :varint, :varint, :string, :string, :varint])

  #   {:ok,
  #    %{
  #      socket: socket,
  #      revision: min(Messages.revision(), revision),
  #      timezone: timezone,
  #      buffer: ""
  #    }}
  # end

  # # TODO await resp? record query id for Choto.stream?
  # @spec query(conn, String.t()) :: :ok
  # def query(conn, query) do
  #   %{socket: socket, revision: revision} = conn

  #   :ok =
  #     :gen_tcp.send(socket, [
  #       Messages.client_query(query, revision),
  #       Messages.client_data([])
  #     ])
  # end

  # @spec send_data(conn, [[term]]) :: :ok
  # def send_data(conn, data) do
  #   %{socket: socket, revision: _revision} = conn

  #   :ok =
  #     :gen_tcp.send(socket, [
  #       # TODO pass revision, a recent version uses 0 between column type and values
  #       Messages.client_data(data),
  #       Messages.client_data([])
  #     ])
  # end

  # @spec ping(conn) :: :ok
  # def ping(conn) do
  #   %{socket: socket} = conn
  #   :ok = :gen_tcp.send(socket, Messages.client_ping())
  # end

  # # TODO
  # @type server_packet :: term

  # @doc "Receives a single server packet. Useful for pings and other similar packets."
  # @spec recv(conn) :: {:ok, server_packet, conn}
  # def recv(conn) do
  #   %{socket: socket, revision: _revision, buffer: buffer} = conn
  #   {:ok, data} = :gen_tcp.recv(socket, 0)

  #   case decode(buffer <> data) do
  #     {:ok, buffer, packet} -> {:ok, packet, %{conn | buffer: buffer}}
  #     {:more, buffer} -> recv(%{conn | buffer: buffer})
  #   end
  # end

  # # TODO handle errors
  # # TODO receive_all?
  # @doc "Receives all packets until end of stream or exception. Useful for queries."
  # @spec await(conn) :: {:ok, [server_packet], conn}
  # def await(conn, acc \\ []) do
  #   # TODO use revision in decode
  #   %{socket: socket, revision: _revision, buffer: buffer} = conn
  #   {:ok, data} = :gen_tcp.recv(socket, 0)

  #   case decode_all(buffer <> data, acc) do
  #     {:more, buffer, inner_acc, acc} -> await_continue(%{conn | buffer: buffer}, inner_acc, acc)
  #     {:more, buffer, acc} -> await(%{conn | buffer: buffer}, acc)
  #     {:done, buffer, acc} -> {:ok, acc, %{conn | buffer: buffer}}
  #   end
  # end

  # def await_continue(conn, inner_acc, acc) do
  #   %{socket: socket, revision: _revision, buffer: buffer} = conn
  #   {:ok, data} = :gen_tcp.recv(socket, 0)

  #   case decode_all_continue(buffer <> data, inner_acc, acc) do
  #     {:more, buffer, inner_acc, acc} -> await_continue(%{conn | buffer: buffer}, inner_acc, acc)
  #     {:more, buffer, acc} -> await(%{conn | buffer: buffer}, acc)
  #     {:done, buffer, acc} -> {:ok, acc, %{conn | buffer: buffer}}
  #   end
  # end

  # defp decode_all(bytes, acc) do
  #   case decode(bytes) do
  #     {:ok, bytes, {:exception, _exception} = exception} ->
  #       {:done, bytes, :lists.reverse([exception | acc])}

  #     {:ok, bytes, :end_of_stream = eos} ->
  #       {:done, bytes, :lists.reverse([eos | acc])}

  #     {:ok, bytes, packet} ->
  #       decode_all(bytes, [packet | acc])

  #     {:more, bytes, inner_acc} ->
  #       {:more, bytes, inner_acc, acc}

  #     :more ->
  #       {:more, bytes, acc}
  #   end
  # end

  # defp decode_all_continue(bytes, {kind, inner_acc}, acc) do
  #   case decode_continue(bytes, kind, inner_acc) do
  #     {:ok, bytes, {:exception, _exception} = exception} ->
  #       {:done, bytes, :lists.reverse([exception | acc])}

  #     {:ok, bytes, :end_of_stream = eos} ->
  #       {:done, bytes, :lists.reverse([eos | acc])}

  #     {:ok, bytes, packet} ->
  #       decode_all(bytes, [packet | acc])

  #     {:more, bytes, inner_acc} ->
  #       {:more, bytes, inner_acc, acc}
  #   end
  # end

  # @spec close(conn) :: :ok
  # def close(%{socket: socket}) do
  #   :gen_tcp.close(socket)
  # end

  # defp decode_continue(bytes, kind, inner_acc) do
  #   case Decoder.decode_block_continue(bytes, inner_acc) do
  #     {:ok, rest, block} -> {:ok, rest, {kind, block}}
  #     {:more, rest, block_acc} -> {:more, rest, {kind, block_acc}}
  #   end
  # end

  # # TODO in decode, handle incomplete blocks / messages
  # # not just {:ok, rest, decoded} = ...

  # @doc false
  # # TODO what is this 0? name? then better decode it properly
  # def decode(<<@server_data, 0, rest::bytes>>) do
  #   case Decoder.decode_block(rest) do
  #     {:ok, rest, block} -> {:ok, rest, {:data, block}}
  #     {:more, rest, block_acc} -> {:more, rest, {:data, block_acc}}
  #   end
  # end

  # def decode(<<@server_profile_info, rest::bytes>>) do
  #   types = [
  #     _rows = :varint,
  #     _blocks = :varint,
  #     _bytes = :varint,
  #     _applied_limit = :boolean,
  #     _rows_before_limit = :varint,
  #     _calculated_rows_before_limit = :boolean
  #   ]

  #   case Decoder.decode(rest, types) do
  #     {:ok, rest, profile_info} -> {:ok, rest, {:profile_info, profile_info}}
  #     {:more, _rest, _types, _inner_acc} -> :more
  #   end
  # end

  # def decode(<<@server_progress, rest::bytes>>) do
  #   types = [
  #     _rows = :varint,
  #     _bytes = :varint,
  #     _total_rows = :varint,
  #     # TODO if revision > DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
  #     _wrote_rows = :varint,
  #     _wrote_bytes = :varint
  #   ]

  #   case Decoder.decode(rest, types) do
  #     {:ok, rest, progress} -> {:ok, rest, {:progress, progress}}
  #     {:more, _rest, _types, _inner_acc} -> :more
  #   end
  # end

  # def decode(<<@server_pong, rest::bytes>>) do
  #   {:ok, rest, :pong}
  # end

  # def decode(<<@server_profile_events, 0, rest::bytes>>) do
  #   case Decoder.decode_block(rest) do
  #     {:ok, rest, profile_events} -> {:ok, rest, {:profile_events, profile_events}}
  #     {:more, rest, block_acc} -> {:more, rest, {:profile_events, block_acc}}
  #   end
  # end

  # def decode(<<@server_end_of_stream, rest::bytes>>) do
  #   {:ok, rest, :end_of_stream}
  # end

  # def decode(<<@server_exception, rest::bytes>>) do
  #   types = [
  #     _code = :i32,
  #     _name = :string,
  #     _message = :string,
  #     _stack_trace = :string,
  #     _has_nested = :boolean
  #   ]

  #   case Decoder.decode(rest, types) do
  #     {:ok, rest, exception} -> {:ok, rest, {:exception, exception}}
  #     {:more, _rest, _types, _inner_acc} -> :more
  #   end
  # end

  # # TODO 0 again?
  # def decode(<<@server_table_columns, 0, rest::bytes>>) do
  #   case Decoder.decode(rest, [:string]) do
  #     {:ok, rest, columns} -> {:ok, rest, {:table_columns, columns}}
  #     {:more, _rest, _types, _inner_acc} -> :more
  #   end
  # end

  # def decode(""), do: :more
end
