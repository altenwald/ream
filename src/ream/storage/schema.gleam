import birl/time
import gleam/bit_string
import gleam/float
import gleam/int
import gleam/map.{Map}
import gleam/option.{Some}
import ream/storage/file as fs
import ream/storage/memtable
import ream/storage/memtable/range.{MemTableRange}
import ream/storage/schema/table.{FieldId, Table}
import ream/storage/sstable
import ream/storage/value.{Value}
import ream/storage/value/index.{ValueIndex}

pub type DataType {
  Integer(Int)
  Float(Float)
  Decimal(Int, Int)
  String(String)
  BitString(BitString)
  Timestamp(Int)
}

pub type DataOperation {
  Field(FieldId)
  Literal(DataType)
}

pub type Operation {
  Equal(DataOperation, DataOperation)
  LesserThan(DataOperation, DataOperation)
  GreaterThan(DataOperation, DataOperation)
  LesserOrEqualThan(DataOperation, DataOperation)
  GreaterOrEqualThan(DataOperation, DataOperation)
  Or(Operation, Operation)
  And(Operation, Operation)
  Not(Operation)
  All
}

pub type Schema {
  Schema(
    base_path: String,
    table: Table,
    memtable_ranges: Map(Int, MemTableRange),
    value_index: ValueIndex,
    memtables_loaded: Int,
    max_memtables_loaded: Int,
    max_memtable_size: Int,
    max_value_size: Int,
  )
}

pub fn create(
  table: Table,
  max_memtable_size: Int,
  max_memtables_loaded: Int,
  max_value_size: Int,
  path: String,
) -> Schema {
  let path = fs.join([path, "schema", table.name])
  let assert Ok(True) = fs.recursive_make_directory(path)

  // load the structure of the table
  let meta_file_path = fs.join([path, "meta"])
  let assert Ok(Nil) = table.flush(table, meta_file_path)

  // load memtables where the primary keys are stored
  let key_dir = fs.join([path, "key"])
  let assert Ok(True) = fs.recursive_make_directory(key_dir)
  let assert #(memtables_loaded, ranges) =
    range.load(key_dir, max_memtable_size)

  // open the files where the values (rows) are stored
  let value_dir = fs.join([path, "value"])
  let value_index = index.load(value_dir, max_value_size)

  Schema(
    path,
    table,
    ranges,
    value_index,
    memtables_loaded,
    max_memtables_loaded,
    max_memtable_size,
    max_value_size,
  )
}

pub fn close(schema: Schema) -> Result(Nil, Nil) {
  let assert Ok(_) = flush(schema)
  index.unload(schema.value_index)
}

pub fn flush(schema: Schema) -> Result(Nil, Nil) {
  let key_dir = fs.join([schema.base_path, "key"])
  let assert Ok(True) = fs.recursive_make_directory(key_dir)
  let assert Ok(_) =
    range.flush(schema.base_path, sstable.Key, schema.memtable_ranges)

  let value_index_dir = fs.join([schema.base_path, "value"])
  let assert Ok(True) = fs.recursive_make_directory(value_index_dir)
  let assert Ok(_) = index.flush(schema.value_index)

  Ok(Nil)
}

pub fn find(
  schema: Schema,
  operation: Operation,
) -> #(Result(List(BitString), Nil), Schema) {
  case operation, schema.table.primary_key {
    // All, _ -> get_all(schema)
    Equal(Field(id1), Literal(key)), [id2] if id1 == id2 -> get(schema, key)
    Equal(Literal(key), Field(id1)), [id2] if id1 == id2 -> get(schema, key)
    _, _ -> todo as "still not implemented"
  }
}

fn datatype_to_string(key: DataType) -> String {
  case key {
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
      datatype_to_string(Float(result))
    }
    String(s) -> s
    Timestamp(timestamp) -> {
      timestamp
      |> time.from_unix()
      |> time.to_iso8601()
    }
  }
}

fn find_range(schema: Schema, key_hash: Int) -> #(Int, Schema) {
  let assert #(range_id, ranges, loaded) =
    range.find(
      schema.base_path,
      schema.memtable_ranges,
      schema.memtables_loaded,
      schema.max_memtables_loaded,
      sstable.Key,
      key_hash,
      schema.max_memtable_size,
    )

  #(
    range_id,
    Schema(..schema, memtable_ranges: ranges, memtables_loaded: loaded),
  )
}

fn get(schema: Schema, key: DataType) -> #(Result(List(BitString), Nil), Schema) {
  let key_str = datatype_to_string(key)
  let key_hash = memtable.hash(key_str)
  let #(range_id, schema) = find_range(schema, key_hash)
  let assert Ok(range) = map.get(schema.memtable_ranges, range_id)
  let assert Some(memtable) = range.memtable
  case memtable.get(memtable, key_str) {
    Ok(value) -> {
      let assert Ok(vfile) = index.get(schema.value_index, value.file_id)
      case value.read(vfile, value.offset) {
        Ok(Value(deleted: False, file_id: _, offset: _, data: Some(data))) -> #(
          Ok([data]),
          schema,
        )
        _ -> #(Error(Nil), schema)
      }
    }
    Error(_err) -> #(Error(Nil), schema)
  }
}
