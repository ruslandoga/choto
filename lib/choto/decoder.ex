# based on https://github.com/clickhouse-elixir/clickhousex/blob/master/lib/clickhousex/codec/binary.ex
defmodule Choto.Decoder do
  import Bitwise
  # @compile {:bin_opt_info, true}

  def decode(bytes, types) do
    _decode(bytes, types, _acc = [])
  end

  # TODO handle incomplete blocks
  def decode_block(<<1, 0, 2, -1::32-little-signed, 0, cols, rows, rest::bytes>>) do
    _decode_block(rest, cols, rows, [])
  end

  # first I'll go back to unoptimised approach with excessive binary copying
  # then I'll use pivot with stack
  # then after benching will decide if something else is needed like varint macros

  defp _decode_block(bytes, cols, rows, acc) when cols > 0 do
    # or just throw?
    case decode(bytes, [:string, :string]) do
      {:ok, bytes, [name, type]} ->
        type =
          case type do
            "UInt8" -> :u8
            "UInt16" -> :u16
            "UInt32" -> :u32
            "UInt64" -> :u64
            "Int8" -> :i8
            "Int16" -> :i16
            "Int32" -> :i32
            "Int64" -> :i64
            "Float32" -> :f32
            "Float64" -> :f64
            # TODO parse enums
            "Enum8" <> _rest -> :u8
            "Enum16" <> _rest -> :u8
            "String" -> :string
            "Date" -> :date
            "DateTime" -> :datetime
          end

        case _decode_times(bytes, type, rows) do
          {:ok, bytes, values} ->
            _decode_block(bytes, cols - 1, rows, [[{name, type} | values] | acc])

          {:more, bytes, types, inner_acc} ->
            {:more, bytes, {name, type, types, inner_acc, cols, rows, acc}}
        end

      {:more, bytes, types, inner_acc} ->
        {:more, bytes, {types, inner_acc, cols, rows, acc}}
    end
  end

  defp _decode_block(bytes, 0, _rows, acc) do
    {:ok, bytes, :lists.reverse(acc)}
  end

  def decode_block_continue(bytes, {name, type, types, inner_acc, cols, rows, acc}) do
    case _decode(bytes, types, inner_acc) do
      {:ok, bytes, values} ->
        _decode_block(bytes, cols - 1, rows, [[{name, type} | values] | acc])

      {:more, bytes, types, inner_acc} ->
        {:more, bytes, {name, type, types, inner_acc, cols, rows, acc}}
    end
  end

  def decode_block_continue(bytes, {types, inner_acc, cols, rows, acc}) do
    case decode(bytes, types) do
      {:ok, bytes, decoded} ->
        # TODO
        [name, type] = :lists.reverse(inner_acc) ++ decoded

        type =
          case type do
            "UInt8" -> :u8
            "UInt16" -> :u16
            "UInt32" -> :u32
            "UInt64" -> :u64
            "Int8" -> :i8
            "Int16" -> :i16
            "Int32" -> :i32
            "Int64" -> :i64
            "Float32" -> :f32
            "Float64" -> :f64
            # TODO parse enums
            "Enum8" <> _rest -> :u8
            "Enum16" <> _rest -> :u8
            "String" -> :string
            "Date" -> :date
            "DateTime" -> :datetime
          end

        case _decode_times(bytes, type, rows) do
          {:ok, bytes, values} ->
            _decode_block(bytes, cols - 1, rows, [[{name, type} | values] | acc])

          {:more, bytes, types, inner_acc} ->
            {:more, bytes, {name, type, types, inner_acc, cols, rows, acc}}
        end
    end
  end

  # TODO
  defp _decode_times(bytes, type, times) do
    _decode(bytes, List.duplicate(type, times), [])
  end

  defp _decode(<<1, rest::bytes>>, [:boolean | types], acc) do
    _decode(rest, types, [true | acc])
  end

  defp _decode(<<0, rest::bytes>>, [:boolean | types], acc) do
    _decode(rest, types, [false | acc])
  end

  defp _decode(bytes, [:varint | types] = og_types, acc) do
    _decode_varint(bytes, types, og_types, acc)
  end

  defp _decode(<<value::64-little-signed-float, rest::bytes>>, [:f64 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::32-little-signed-float, rest::bytes>>, [:f32 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(bytes, [:string | types] = og_types, acc) do
    _decode_string(bytes, types, og_types, acc)
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

  defp _decode(<<value::64-little, rest::bytes>>, [:u64 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::32-little, rest::bytes>>, [:u32 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::16-little, rest::bytes>>, [:u16 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  defp _decode(<<value::little, rest::bytes>>, [:u8 | types], acc) do
    _decode(rest, types, [value | acc])
  end

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  defp _decode(<<days_since_epoch::16-little, rest::bytes>>, [:date | types], acc) do
    date = Date.add(@epoch_date, days_since_epoch)
    _decode(rest, types, [date | acc])
  end

  defp _decode(<<seconds_since_epoch::32-little, rest::bytes>>, [:datetime | types], acc) do
    date_time = NaiveDateTime.add(@epoch_naive_datetime, seconds_since_epoch)
    _decode(rest, types, [date_time | acc])
  end

  defp _decode(rest, _types = [], acc) do
    {:ok, rest, :lists.reverse(acc)}
  end

  # TODO
  defp _decode(bytes, types, acc) do
    {:more, bytes, types, acc}
  end

  varints = [
    {
      quote(do: <<0::1, v1::7>>),
      quote(do: v1)
    },
    {
      quote(do: <<1::1, v1::7, 0::1, v2::7>>),
      quote(do: (v2 <<< 7) + v1)
    },
    {
      quote(do: <<1::1, v1::7, 1::1, v2::7, 0::1, v3::7>>),
      quote(do: (v3 <<< 14) + (v2 <<< 7) + v1)
    },
    {
      quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 0::1, v4::7>>),
      quote(do: (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)
    },
    {
      quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 0::1, v5::7>>),
      quote(do: (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)
    },
    {
      quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 0::1, v6::7>>),
      quote(do: (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)
    },
    {
      quote(
        do:
          <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 0::1,
            v7::7>>
      ),
      quote(
        do: (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1
      )
    },
    {
      quote(
        do:
          <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 1::1,
            v7::7, 0::1, v8::7>>
      ),
      quote(
        do:
          (v7 <<< 49) + (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) +
            (v2 <<< 7) + v1
      )
    }
  ]

  for {input, output} <- varints do
    defp _decode_varint(<<unquote(input), rest::bytes>>, types, _, acc) do
      _decode(rest, types, [unquote(output) | acc])
    end
  end

  defp _decode_varint(rest, _, types, acc) do
    {:more, rest, types, acc}
  end

  strings =
    for {input, output} <- varints do
      quote(do: <<unquote(input), v::size(unquote(output))-bytes>>)
    end

  for input <- strings do
    defp _decode_string(<<unquote(input), rest::bytes>>, types, _, acc) do
      _decode(rest, types, [unquote(quote(do: v)) | acc])
    end
  end

  defp _decode_string(rest, _, types, acc) do
    {:more, rest, types, acc}
  end
end
