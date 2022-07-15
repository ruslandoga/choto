defmodule Choto.Messages do
  alias Choto.{Encoder, Decoder}

  @client_hello 0
  @client_query 1
  # @client_data 2
  # @client_cancel 3
  @client_ping 4
  # @client_table_status 5
  @server_hello 0
  # @server_data 1
  @server_exception 2
  # @server_progress 3
  @server_pong 4
  # @server_end_of_stream 5
  # @server_profile_info 6
  # @server_totals 7
  # @server_extremes 8
  # @server_table_status_response 9
  # @server_log 10

  # @min_revision_with_client_info 54032
  # @min_revision_with_server_timezone 54058
  # @min_revision_with_quota_key_in_client_info 54060
  # @min_revision_with_server_display_name 54372
  # @min_revision_with_version_patch 54401
  # @min_revision_with_client_write_info 54420
  # @min_revision_with_settings_serialized_as_strings 54429
  # @min_revision_with_interserver_secret 54441
  # @min_revision_with_opentelemetry 54442
  # @min_protocol_version_with_distributed_depth 54448
  # @min_protocol_version_with_initial_query_start_time 54449
  # @min_protocol_version_with_incremental_profile_events 54451
  @min_revision_with_parallel_replicas 54453
  @tcp_protocol_version @min_revision_with_parallel_replicas

  def client_hello(database_name, username, password) do
    [
      Encoder.encode(:varint, @client_hello),
      Encoder.encode(:string, "choto"),
      Encoder.encode(:varint, 1),
      Encoder.encode(:varint, 1),
      # 54453

      Encoder.encode(:varint, _client_revision = 54456),
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

  def client_query(query, query_id \\ "") do
    [
      Encoder.encode(:varint, @client_query),
      Encoder.encode(:string, query_id),
      encode_client_info(new_client_info(@query_kind_initial_query)),
      _settings = [],
      # empty string is a marker of the end of settin
      Encoder.encode(:string, ""),
      # if revision > @min_revision_with_interserver_secret do
      Encoder.encode(:string, ""),
      # else
      # []
      # end,
      # 2 is query processing state complete
      Encoder.encode(:varint, 2),
      Encoder.encode(:boolean, _compression = false),
      Encoder.encode(:string, query)
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

  def encode_client_info(info) do
    [
      Encoder.encode(:u8, info.query_kind),
      Encoder.encode(:string, info.initial_user),
      Encoder.encode(:string, info.initial_query_id),
      Encoder.encode(:string, info.initial_address),
      # TODO revision >= @min_protocol_version_with_initial_query_start_time
      # initial_query_start_time_microseconds
      Encoder.encode(:u64, 0),
      Encoder.encode(:u8, info.interface),
      Encoder.encode(:string, info.os_user),
      Encoder.encode(:string, info.client_hostname),
      Encoder.encode(:string, info.client_name),
      Encoder.encode(:varint, info.version_major),
      Encoder.encode(:varint, info.version_minor),
      Encoder.encode(:varint, info.revision),
      # revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
      Encoder.encode(:string, info.quota_key),
      # revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
      Encoder.encode(:varint, 0),
      # revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH
      Encoder.encode(:varint, info.patch),
      # revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY
      Encoder.encode(:u8, 0),
      # revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS
      # collaborate_with_initiator
      Encoder.encode(:varint, 0),
      # count_participating_replicas
      Encoder.encode(:varint, 0),
      # number_of_current_replica
      Encoder.encode(:varint, 0)
    ]
  end

  def decode(<<@server_hello, rest::bytes>>) do
    Decoder.decode(
      rest,
      :struct,
      _spec = [
        server_name: :string,
        server_version_major: :varint,
        server_version_minor: :varint,
        server_revision: :varint,
        # >= 54058
        server_timezone: :string,
        # >= 54372
        server_display_name: :string,
        server_version_patch: :varint
      ]
    )
  end

  def decode(<<@server_exception, rest::bytes>>) do
    Decoder.decode(
      rest,
      :struct,
      _spec = [
        code: :i32,
        name: :string,
        message: :string,
        stack_trace: :string,
        has_nested: :boolean
      ]
    )
  end

  def decode(<<@server_pong, rest::bytes>>) do
    {:ok, :pong, rest}
  end
end
