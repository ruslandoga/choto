# TODO maybe just Choto.Codec with both primitive types and "messages"
defmodule Choto.Messages do
  alias Choto.Encoder

  # see https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Protocol.h

  #######################
  # client packet types #
  #######################

  # Name, version, revision, default DB
  @client_hello 0

  # Query id, query settings, stage up to which the query must be executed,
  # whether the compression must be used,
  # query text (without data for INSERTs).
  @client_query 1

  # A block of data (compressed or not).
  @client_data 2

  # Cancel the query execution.
  # @client_cancel 3

  # Check that connection to the server is alive.
  @client_ping 4

  # Check status of tables on the server.
  # @client_table_status 5

  # Keep the connection alive
  # @client_keep_alive 6

  # A block of data (compressed or not).
  # @client_scalar 7

  # List of unique parts ids to exclude from query processing
  # @client_ignored_part_uuids 8

  # A filename to read from s3 (used in s3Cluster)
  # @client_read_task_response 9

  # Coordinator's decision with a modified set of mark ranges allowed to read
  # @client_merge_tree_read_task_response 10

  # see https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ProtocolDefines.h

  # @min_revision_with_client_info 54032
  # @min_revision_with_server_timezone 54058
  @min_revision_with_quota_key_in_client_info 54060
  # @min_revision_with_server_display_name 54372
  @min_revision_with_version_patch 54401
  # @min_revision_with_client_write_info 54420
  # @min_revision_with_settings_serialized_as_strings 54429
  @min_revision_with_interserver_secret 54441
  @min_revision_with_opentelemetry 54442
  @min_protocol_version_with_distributed_depth 54448
  @min_protocol_version_with_initial_query_start_time 54449
  # @min_protocol_version_with_incremental_profile_events 54451
  @min_revision_with_parallel_replicas 54453
  @tcp_protocol_version @min_revision_with_parallel_replicas

  def revision, do: @tcp_protocol_version

  def client_hello(database_name, username, password) do
    [
      Encoder.encode(:varint, @client_hello),
      Encoder.encode(:string, "choto"),
      Encoder.encode(:varint, 1),
      Encoder.encode(:varint, 1),
      Encoder.encode(:varint, _client_revision = @tcp_protocol_version),
      Encoder.encode(:string, database_name),
      Encoder.encode(:string, username),
      Encoder.encode(:string, password)
    ]
  end

  def client_ping do
    [Encoder.encode(:varint, @client_ping)]
  end

  # @query_kind_no_query 0
  @query_kind_initial_query 1
  # @query_kind_secondary_query 2

  # unlike `if` only supports bools and returns [] in place of nil
  defmacrop if_supported(condition, do: block) do
    quote do
      case unquote(condition) do
        true -> unquote(block)
        false -> []
      end
    end
  end

  def client_query(query, revision, query_id \\ "") do
    [
      Encoder.encode(:varint, @client_query),
      Encoder.encode(:string, query_id),
      encode_client_info(new_client_info(@query_kind_initial_query), revision),
      # TODO
      _settings = [],
      # empty string is a marker of the end of settings
      Encoder.encode(:string, ""),
      if_supported revision >= @min_revision_with_interserver_secret do
        Encoder.encode(:string, "")
      end,
      Encoder.encode(:varint, _state_complete = 2),
      # TODO we have nimble_lz4, so can enable compression, compare performance
      Encoder.encode(:boolean, _compression = false),
      Encoder.encode(:string, query)
    ]
  end

  def client_data do
    [
      Encoder.encode(:varint, @client_data),
      # TODO what is it?
      0,
      encode_block()
    ]
  end

  def encode_block do
    [
      encode_block_info(),
      _num_columns = 0,
      _num_rows = 0
    ]
  end

  # https://github.com/vahid-sohrabloo/chconn/blob/68e4cebc13c147da2ccec37dec433761ae041ebb/block.go#L168
  # and https://github.com/ClickHouse/clickhouse-go/blob/5c3b7a7c44f03422165d319d330d05272d4ebc33/lib/proto/block.go#L170
  def encode_block_info do
    [
      Encoder.encode(:varint, 1),
      # TODO
      Encoder.encode(:u8, _is_overflow = 0),
      Encoder.encode(:varint, 2),
      # TODO
      Encoder.encode(:i32, _bucket_num = -1),
      Encoder.encode(:varint, 0)
    ]
  end

  def new_client_info(query_kind) do
    {:ok, hostname} = :inet.gethostname()

    %{
      client_hostname: to_string(hostname),
      client_name: "choto",
      initial_address: "0.0.0.0:0",
      initial_query_id: "",
      initial_user: "",
      interface: _tcp = 1,
      os_user: System.get_env("USER"),
      patch: 3,
      quota_key: "",
      revision: @tcp_protocol_version,
      version_major: 1,
      version_minor: 1,
      query_kind: query_kind
    }
  end

  def encode_client_info(info, revision) do
    [
      Encoder.encode(:u8, info.query_kind),
      Encoder.encode(:string, info.initial_user),
      Encoder.encode(:string, info.initial_query_id),
      Encoder.encode(:string, info.initial_address),
      if_supported revision >= @min_protocol_version_with_initial_query_start_time do
        Encoder.encode(:u64, _initial_query_start_time_microseconds = 0)
      end,
      Encoder.encode(:u8, info.interface),
      Encoder.encode(:string, info.os_user),
      Encoder.encode(:string, info.client_hostname),
      Encoder.encode(:string, info.client_name),
      Encoder.encode(:varint, info.version_major),
      Encoder.encode(:varint, info.version_minor),
      Encoder.encode(:varint, info.revision),
      if_supported revision >= @min_revision_with_quota_key_in_client_info do
        Encoder.encode(:string, info.quota_key)
      end,
      if_supported revision >= @min_protocol_version_with_distributed_depth do
        Encoder.encode(:varint, 0)
      end,
      if_supported revision >= @min_revision_with_version_patch do
        Encoder.encode(:varint, info.patch)
      end,
      if_supported revision >= @min_revision_with_opentelemetry do
        Encoder.encode(:u8, 0)
      end,
      if_supported revision >= @min_revision_with_parallel_replicas do
        [
          Encoder.encode(:varint, _collaborate_with_initiator = 0),
          Encoder.encode(:varint, _count_participating_replicas = 0),
          Encoder.encode(:varint, _number_of_current_replica = 0)
        ]
      end
    ]
  end
end
