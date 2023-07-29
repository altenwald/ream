import gleam/erlang/file
import gleam/bit_string
import gleam/list
import gleam/string
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

pub type FieldId =
  Int

pub type Field {
  Field(id: FieldId, name: String, data_type: DataType, nilable: Bool)
}

pub type Index {
  Unique(List(FieldId))
  Index(List(FieldId))
}

pub type Table {
  Table(
    name: String,
    fields: List(Field),
    primary_key: List(FieldId),
    indexes: List(Index),
  )
}

pub const name_size_bits = 8

pub const field_id_size_bits = 16

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
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  let name_size = string.byte_size(table.name)
  let num_fields = list.length(table.fields)
  let fields =
    table.fields
    |> list.fold(
      <<num_fields:size(field_id_size_bits)>>,
      fn(acc, field) { bit_string.append(acc, field_to_bitstring(field)) },
    )
  let primary_keys = field_ids_to_bitstring(table.primary_key)
  let indexes = indexes_to_bitstring(table.indexes)
  <<
    name_size:size(name_size_bits),
    table.name:utf8,
    fields:bit_string,
    primary_keys:bit_string,
    indexes:bit_string,
  >>
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

fn field_ids_to_bitstring(ids: List(FieldId)) -> BitString {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  let ids_num = list.length(ids)
  list.fold(
    ids,
    <<ids_num:size(field_id_size_bits)>>,
    fn(acc, id) { <<acc:bit_string, id:size(field_id_size_bits)>> },
  )
}

fn indexes_to_bitstring(indexes: List(Index)) -> BitString {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  let idx_num = list.length(indexes)
  list.fold(
    indexes,
    <<idx_num:size(field_id_size_bits)>>,
    fn(acc, index) {
      case index {
        Unique(field_ids) -> {
          let field_ids = field_ids_to_bitstring(field_ids)
          <<acc:bit_string, 0:8, field_ids:bit_string>>
        }
        Index(field_ids) -> {
          let field_ids = field_ids_to_bitstring(field_ids)
          <<acc:bit_string, 1:8, field_ids:bit_string>>
        }
      }
    },
  )
}

pub fn from_bitstring(data: BitString) -> Table {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let name_size_bits = name_size_bits
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  let <<name_size:size(name_size_bits), rest:bit_string>> = data
  let name_size_bits = name_size * 8
  let <<
    table_name:size(name_size_bits)-bit_string,
    num_fields:size(field_id_size_bits),
    rest:bit_string,
  >> = rest
  let #(fields, <<num_ids:size(field_id_size_bits), rest:bit_string>>) =
    fields_from_bitstring(num_fields, rest, 1, [])
  let #(ids, <<num_idx:size(field_id_size_bits), rest:bit_string>>) =
    ids_from_bitstring(num_ids, rest, [])
  let #(idx, <<>>) = indexes_from_bitstring(num_idx, rest, [])
  let assert Ok(name) = bit_string.to_string(table_name)
  Table(name: name, fields: fields, primary_key: ids, indexes: idx)
}

fn indexes_from_bitstring(
  num_idx: Int,
  data: BitString,
  acc: List(Index),
) -> #(List(Index), BitString) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  case num_idx, data {
    0, _ -> #(list.reverse(acc), data)
    _, <<0:8, num_ids:size(field_id_size_bits), rest:bit_string>> -> {
      let #(ids, rest) = ids_from_bitstring(num_ids, rest, [])
      indexes_from_bitstring(num_idx - 1, rest, [Unique(ids), ..acc])
    }
    _, <<1:8, num_ids:size(field_id_size_bits), rest:bit_string>> -> {
      let #(ids, rest) = ids_from_bitstring(num_ids, rest, [])
      indexes_from_bitstring(num_idx - 1, rest, [Index(ids), ..acc])
    }
  }
}

fn ids_from_bitstring(
  num_ids: Int,
  data: BitString,
  acc: List(FieldId),
) -> #(List(FieldId), BitString) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let field_id_size_bits = field_id_size_bits
  // end FIXME
  case num_ids {
    0 -> #(list.reverse(acc), data)
    _ -> {
      let <<id:size(field_id_size_bits), rest:bit_string>> = data
      ids_from_bitstring(num_ids - 1, rest, [id, ..acc])
    }
  }
}

fn fields_from_bitstring(
  num_fields: Int,
  data: BitString,
  id: Int,
  acc: List(Field),
) -> #(List(Field), BitString) {
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
  case num_fields - id {
    0 -> #(list.reverse([field, ..acc]), rest)
    _ -> fields_from_bitstring(num_fields, rest, id + 1, [field, ..acc])
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
