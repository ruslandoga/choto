Benchee.run(
  %{
    "decode" => fn {data, types} -> Choto.Decoder.decode(data, types) end
  },
  inputs: %{
    "server hello" =>
      {_data = "\nClickHouse\x16\a\xB8\xA9\x03\rEurope/Moscow\nmac3.local\x01",
       _types = [
         _name = :string,
         _version_major = :varint,
         _version_minor = :varint,
         _revision = :varint,
         _timezone = :string,
         _display_name = :string,
         _version_patch = :varint
       ]},
    "cli query" =>
      {_data =
         "\x01$011b7efb-9127-40f1-87d5-b5404c19675c\x01\0$011b7efb-9127-40f1-87d5-b5404c19675c\t0.0.0.0:0\0\0\0\0\0\0\0\0\x01\x01q\nmac3.local\nClickHouse\x16\a\xB8\xA9\x03\0\0\x01\0\0\0\0\0\0\x02\0\rselect 1 + 1;\x02\0\x01\0\x02\xFF\xFF\xFF\xFF\0\0\0",
       _types = [
         _code = :varint,
         _query_id = :string,
         _query_kind = :u8,
         _initial_user = :string,
         _initial_query_id = :string,
         _initial_address = :string,
         _timestamp = :i64,
         _interface = :u8,
         _os_user = :string,
         _client_hostname = :string,
         _client_name = :string,
         _version_major = :varint,
         _version_minor = :varint,
         _revision = :varint,
         _quota_key = :string,
         _distributed_depth = :varint,
         _version_patch = :varint,
         _open_telemetry = :u8,
         _collaborate_with_initiator = :varint,
         _count_participating_replicas = :varint,
         _number_of_current_replica = :varint,
         _settings_end = :string,
         _interserver_secret = :string,
         _query_state = :varint,
         _compression = :boolean,
         _query = :string
       ]}
  }
)
