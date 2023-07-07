import gleam/erlang/file
import gleam/bit_string
import gleam/list
import ream/storage/file as fs

pub type Size {
  Size(Int)
  Unlimited
}

pub type DataType {
  Integer
  Float
  Decimal
  String(Size)
  BitString(Size)
  Timestamp
}

pub type Field {
  Field(id: Int, name: String, data_type: DataType, nilable: Bool)
}

pub type Table {
  Table(name: String, fields: List(Field))
}

pub const name_size_bits = 8

pub fn flush(table: Table, path: String) -> Result(Nil, file.Reason) {
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(path))
  file.write_bits(to_bitstring(table), path)
}

pub fn load(path: String) -> Result(Table, file.Reason) {
  case file.read_bits(path) {
    Ok(data) -> Ok(from_bitstring(data))
    Error(reason) -> Error(reason)
  }
}

pub fn to_bitstring(table: Table) -> BitString {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let name_size_bits = name_size_bits
  // end FIXME
  // FIXME when using Gleam 0.29.2 or later
  // let name_size = string.byte_size(table.name)
  let name_size =
    bit_string.from_string(table.name)
    |> bit_string.byte_size()
  // end FIXME
  let fields =
    table.fields
    |> list.fold(
      <<>>,
      fn(acc, field) { bit_string.append(acc, field_to_bitstring(field)) },
    )
  <<name_size:size(name_size_bits), table.name:utf8, fields:bit_string>>
}

fn field_type_to_bitstring(data_type: DataType) -> BitString {
  case data_type {
    Integer -> <<0:8, 0:8>>
    Float -> <<1:8, 0:8>>
    Decimal -> <<2:8, 0:8>>
    String(Size(size)) -> <<3:8, size:8>>
    String(Unlimited) -> <<3:8, 0:8>>
    BitString(Size(size)) -> <<4:8, size:8>>
    BitString(Unlimited) -> <<4:8, 0:8>>
    Timestamp -> <<5:8, 0:8>>
  }
}

fn field_to_bitstring(field: Field) -> BitString {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let name_size_bits = name_size_bits
  // end FIXME
  let field_name_size_bytes =
    bit_string.from_string(field.name)
    |> bit_string.byte_size()
  <<
    field_name_size_bytes:size(name_size_bits),
    field.name:utf8,
    field_type_to_bitstring(field.data_type):bit_string,
    case field.nilable {
      True -> 1
      False -> 0
    }:size(8),
  >>
}

pub fn from_bitstring(data: BitString) -> Table {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let name_size_bits = name_size_bits
  // end FIXME
  let <<name_size:size(name_size_bits), rest:bit_string>> = data
  let name_size_bits = name_size * 8
  let <<table_name:size(name_size_bits)-bit_string, fields:bit_string>> = rest
  let assert Ok(name) = bit_string.to_string(table_name)
  Table(name: name, fields: fields_from_bitstring(fields, 1, []))
}

fn fields_from_bitstring(
  data: BitString,
  id: Int,
  acc: List(Field),
) -> List(Field) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let name_size_bits = name_size_bits
  // end FIXME
  let <<field_name_size_bytes:size(name_size_bits), field_and_rest:bit_string>> =
    data
  let field_name_size_bits = field_name_size_bytes * 8
  let <<
    field_name:size(field_name_size_bits)-bit_string,
    field_type:size(16)-bit_string,
    field_nilable:size(8),
    rest:bit_string,
  >> = field_and_rest
  let assert Ok(name) = bit_string.to_string(field_name)
  let field =
    Field(
      id: id,
      name: name,
      data_type: field_type_from_bitstring(field_type),
      nilable: case field_nilable {
        1 -> True
        0 -> False
      },
    )
  case rest {
    <<>> -> list.reverse([field, ..acc])
    _ -> fields_from_bitstring(rest, id + 1, [field, ..acc])
  }
}

fn field_type_from_bitstring(data_type: BitString) -> DataType {
  case data_type {
    <<0:8, 0:8>> -> Integer
    <<1:8, 0:8>> -> Float
    <<2:8, 0:8>> -> Decimal
    <<3:8, 0:8>> -> String(Unlimited)
    <<3:8, size:8>> -> String(Size(size))
    <<4:8, 0:8>> -> BitString(Unlimited)
    <<4:8, size:8>> -> BitString(Size(size))
    <<5:8, 0:8>> -> Timestamp
  }
}
