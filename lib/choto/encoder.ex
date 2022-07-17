# based on https://github.com/clickhouse-elixir/clickhousex/blob/master/lib/clickhousex/codec/binary.ex
defmodule Choto.Encoder do
  use Bitwise

  def encode(:varint, num) when num < 128, do: <<num>>
  # TODO
  def encode(:varint, num), do: <<1::1, num::7, encode(:varint, num >>> 7)::bytes>>

  def encode(:string, str) do
    [encode(:varint, byte_size(str)) | str]
  end

  # TODO is it always little endian , even if machine is big endian? should it be native endian?

  def encode(:u8, i), do: <<i::little-unsigned>>
  def encode(:u16, i), do: <<i::16-little-unsigned>>
  def encode(:u32, i), do: <<i::32-little-unsigned>>
  def encode(:u64, i), do: <<i::64-little-unsigned>>

  def encode(:i8, i), do: <<i::little-signed>>
  def encode(:i16, i), do: <<i::16-little-signed>>
  def encode(:i32, i), do: <<i::32-little-signed>>
  def encode(:i64, i), do: <<i::64-little-signed>>

  def encode(:boolean, true), do: encode(:u8, 1)
  def encode(:boolean, false), do: encode(:u8, 0)
end
