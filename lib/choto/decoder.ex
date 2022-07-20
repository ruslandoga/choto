# based on https://github.com/clickhouse-elixir/clickhousex/blob/master/lib/clickhousex/codec/binary.ex
defmodule Choto.Decoder do
  use Bitwise
  # @compile {:bin_opt_info, true}

  def decode(bytes, types) do
    _decode(bytes, types, _acc = [])
  end

  defp _decode(<<1, rest::bytes>>, [:boolean | types], acc) do
    _decode(rest, types, [true | acc])
  end

  defp _decode(<<0, rest::bytes>>, [:boolean | types], acc) do
    _decode(rest, types, [false | acc])
  end

  defp _decode(bytes, [:varint | types], acc) do
    _decode_varint(bytes, 0, 0, types, acc)
  end

  defp _decode(<<value::64-little-signed-float, rest::bytes>>, [:f64 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::32-little-signed-float, rest::bytes>>, [:f32 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(bytes, [:string | types], acc) do
    _decode_string(bytes, types, acc)
  end

  defp _decode(<<value::64-little-signed, rest::bytes>>, [:i64 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::32-little-signed, rest::bytes>>, [:i32 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::16-little-signed, rest::bytes>>, [:i16 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::little-signed, rest::bytes>>, [:i8 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::64-little-unsigned, rest::bytes>>, [:u64 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::32-little-unsigned, rest::bytes>>, [:u32 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::16-little-unsigned, rest::bytes>>, [:u16 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::little-unsigned, rest::bytes>>, [:u8 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  defp _decode(<<days_since_epoch::16-little-unsigned, rest::bytes>>, [:date | types], acc) do
    date = Date.add(@epoch_date, days_since_epoch)
    _decode(rest, types, [date | acc])
  end

  defp _decode(<<seconds_since_epoch::32-little-unsigned, rest::bytes>>, [:datetime | types], acc) do
    date_time = NaiveDateTime.add(@epoch_naive_datetime, seconds_since_epoch)
    _decode(rest, types, [date_time | acc])
  end

  defp _decode(rest, _types = [], acc) do
    {:ok, rest, :lists.reverse(acc)}
  end

  # TODO not necessary <<>>
  defp _decode(<<>>, types, acc) do
    {:resume, types, acc}
  end

  defp _decode_varint(<<1::1, value::7, rest::bytes>>, shift, prev_value, types, acc) do
    _decode_varint(rest, shift + 7, (value <<< shift) + prev_value, types, acc)
  end

  defp _decode_varint(<<0::1, value::7, rest::bytes>>, shift, prev_value, types, acc) do
    _decode(rest, types, [prev_value + (value <<< shift) | acc])
  end

  @compile inline: [_decode_string: 3]
  defp _decode_string(bytes, types, acc) do
    _decode_string_lenght(bytes, 0, 0, types, acc)
  end

  defp _decode_string_lenght(<<1::1, value::7, rest::bytes>>, shift, prev_value, types, acc) do
    _decode_string_lenght(rest, shift + 7, (value <<< shift) + prev_value, types, acc)
  end

  defp _decode_string_lenght(<<0::1, value::7, rest::bytes>>, shift, prev_value, types, acc) do
    len = prev_value + (value <<< shift)

    case rest do
      <<string::size(len)-bytes, rest::bytes>> ->
        _decode(rest, types, [string | acc])

      _other ->
        # TOOD don't lose string
        {:resume, types, acc}
    end
  end
end
