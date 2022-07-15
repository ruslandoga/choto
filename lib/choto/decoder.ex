# based on https://github.com/clickhouse-elixir/clickhousex/blob/master/lib/clickhousex/codec/binary.ex
defmodule Choto.Decoder do
  use Bitwise

  @compile {:bin_opt_info, true}

  def decode(bytes, :struct, spec) do
    decode_struct(bytes, spec, [])
  end

  def decode(bytes, :varint) do
    decode_varint(bytes, 0, 0)
  end

  def decode(bytes, :string) do
    case decode(bytes, :varint) do
      {:ok, byte_count, rest} ->
        case byte_size(rest) >= byte_count do
          true ->
            <<decoded_str::size(byte_count)-bytes, rest::bytes>> = rest
            {:ok, decoded_str, rest}

          false ->
            {:error, {:invalid_string, bytes}}
        end

      {:error, _} = fail ->
        fail
    end
  end

  def decode(<<1, rest::bytes>>, :boolean) do
    {:ok, true, rest}
  end

  def decode(<<0, rest::bytes>>, :boolean) do
    {:ok, false, rest}
  end

  def decode(bytes, {:list, data_type}) do
    {:ok, count, rest} = decode(bytes, :varint)
    decode_list(rest, data_type, count, [])
  end

  def decode(<<decoded::size(64)-little-signed, rest::bytes>>, :i64), do: {:ok, decoded, rest}
  def decode(<<decoded::size(32)-little-signed, rest::bytes>>, :i32), do: {:ok, decoded, rest}
  def decode(<<decoded::size(16)-little-signed, rest::bytes>>, :i16), do: {:ok, decoded, rest}
  def decode(<<decoded::size(8)-little-signed, rest::bytes>>, :i8), do: {:ok, decoded, rest}

  def decode(<<decoded::size(64)-little-unsigned, rest::bytes>>, :u64), do: {:ok, decoded, rest}
  def decode(<<decoded::size(32)-little-unsigned, rest::bytes>>, :u32), do: {:ok, decoded, rest}
  def decode(<<decoded::size(16)-little-unsigned, rest::bytes>>, :u16), do: {:ok, decoded, rest}
  def decode(<<decoded::size(8)-little-unsigned, rest::bytes>>, :u8), do: {:ok, decoded, rest}

  def decode(<<decoded::size(64)-little-signed-float, rest::bytes>>, :f64) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::size(32)-little-signed-float, rest::bytes>>, :f32) do
    {:ok, decoded, rest}
  end

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  def decode(<<days_since_epoch::size(16)-little-unsigned, rest::bytes>>, :date) do
    date = Date.add(@epoch_date, days_since_epoch)
    {:ok, date, rest}
  end

  def decode(<<seconds_since_epoch::size(32)-little-unsigned, rest::bytes>>, :datetime) do
    date_time = NaiveDateTime.add(@epoch_naive_datetime, seconds_since_epoch)
    {:ok, date_time, rest}
  end

  defp decode_list(rest, _, 0, accum) do
    {:ok, Enum.reverse(accum), rest}
  end

  defp decode_list(bytes, data_type, count, accum) do
    case decode(bytes, data_type) do
      {:ok, decoded, rest} -> decode_list(rest, data_type, count - 1, [decoded | accum])
      other -> other
    end
  end

  defp decode_varint(<<0::size(1), byte::size(7), rest::bytes>>, result, shift) do
    {:ok, result ||| byte <<< shift, rest}
  end

  defp decode_varint(<<1::1, byte::7, rest::bytes>>, result, shift) do
    decode_varint(rest, result ||| byte <<< shift, shift + 7)
  end

  defp decode_struct(rest, [], result) do
    {:ok, :lists.reverse(result), rest}
  end

  defp decode_struct(rest, [{field_name, type} | specs], acc) do
    case decode(rest, type) do
      {:ok, decoded, rest} ->
        decode_struct(rest, specs, [{field_name, decoded} | acc])

      {:error, _} = err ->
        err
    end
  end
end
