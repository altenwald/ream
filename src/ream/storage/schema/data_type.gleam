import birl/time
import gleam/bit_string
import gleam/float
import gleam/int
import gleam/result
import gleam/string

pub type DataType {
  Null
  Integer(Int)
  Float(Float)
  Decimal(Int, Int)
  String(String)
  BitString(BitString)
  Timestamp(Int)
}

fn int_byte_size(num: Int) -> Int {
  num
  |> int.to_base2()
  |> string.byte_size()
  |> int.add(7)
  |> int.divide(8)
  |> result.unwrap(0)
}

pub fn to_bitstring(data: DataType) -> BitString {
  case data {
    Integer(i) -> {
      let byte_size = int_byte_size(i)
      let bit_size = byte_size * 8
      <<0:8, byte_size:16, i:size(bit_size)>>
    }
    Float(f) -> <<1:8, f:float>>
    Decimal(d1, d2) -> {
      let d1_byte_size = int_byte_size(d1)
      let d1_bit_size = d1_byte_size * 8
      let d2_byte_size = int_byte_size(d2)
      let d2_bit_size = d2_byte_size * 8
      <<
        2:8,
        d1_byte_size:16,
        d1:size(d1_bit_size),
        d2_byte_size:16,
        d2:size(d2_bit_size),
      >>
    }
    String(s) -> {
      let byte_size = string.byte_size(s)
      <<3:8, byte_size:32, s:utf8>>
    }
    BitString(b) -> {
      let byte_size = bit_string.byte_size(b)
      <<4:8, byte_size:32, b:bit_string>>
    }
    Timestamp(timestamp) -> {
      let byte_size = int_byte_size(timestamp)
      let bit_size = byte_size * 8
      <<5:8, byte_size:16, timestamp:size(bit_size)>>
    }
    Null -> <<6:8>>
  }
}

pub fn from_bitstring(data: BitString) -> #(DataType, BitString) {
  case data {
    <<0:8, byte_size:16, rest:bit_string>> -> {
      let bit_size = byte_size * 8
      let <<int:size(bit_size), rest:bit_string>> = rest
      #(Integer(int), rest)
    }
    <<1:8, f:float, rest:bit_string>> -> #(Float(f), rest)
    <<2:8, d1_byte_size:16, rest:bit_string>> -> {
      let d1_bit_size = d1_byte_size * 8
      let assert <<d1:size(d1_bit_size), d2_byte_size:16, rest:bit_string>> =
        rest
      let d2_bit_size = d2_byte_size * 8
      let assert <<d2:size(d2_bit_size), rest:bit_string>> = rest
      #(Decimal(d1, d2), rest)
    }
    <<3:8, byte_size:32, rest:bit_string>> -> {
      let bit_size = byte_size * 8
      let assert <<str:size(bit_size)-bit_string, rest:bit_string>> = rest
      let assert Ok(str) = bit_string.to_string(str)
      #(String(str), rest)
    }
    <<4:8, byte_size:32, rest:bit_string>> -> {
      let bit_size = byte_size * 8
      let assert <<b:size(bit_size)-bit_string, rest:bit_string>> = rest
      #(BitString(b), rest)
    }
    <<5:8, byte_size:16, rest:bit_string>> -> {
      let bit_size = byte_size * 8
      let assert <<ts:size(bit_size), rest:bit_string>> = rest
      #(Timestamp(ts), rest)
    }
    <<6:8, rest:bit_string>> -> #(Null, rest)
  }
}

pub fn to_string(data: DataType) -> String {
  case data {
    Null -> ""
    Integer(i) -> int.to_string(i)
    Float(f) -> float.to_string(f)
    BitString(b) -> {
      let assert Ok(str) = bit_string.to_string(b)
      str
    }
    Decimal(d1, d2) -> {
      let assert Ok(d) = int.power(10, int.to_float(d2))
      // TODO use some kind of Decimal external library or custom implementation instead
      let assert Ok(result) = float.divide(int.to_float(d1), d)
      to_string(Float(result))
    }
    String(s) -> s
    Timestamp(timestamp) -> {
      timestamp
      |> time.from_unix()
      |> time.to_iso8601()
    }
  }
}
