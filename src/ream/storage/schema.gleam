import gleam/list
import gleam/map.{Map}
import gleam/option.{Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/memtable.{CapacityExceeded, MemTable}
import ream/storage/memtable/range.{MemTableRange}
import ream/storage/schema/table.{FieldId, Table}
import ream/storage/schema/data_type.{DataType}
import ream/storage/sstable
import ream/storage/value.{Value}
import ream/storage/value/index.{ValueIndex}

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
  let assert Ok(_) = range.flush(key_dir, sstable.Key, schema.memtable_ranges)

  let value_index_dir = fs.join([schema.base_path, "value"])
  let assert Ok(True) = fs.recursive_make_directory(value_index_dir)
  let assert Ok(_) = index.flush(schema.value_index)

  Ok(Nil)
}

pub fn insert(
  schema: Schema,
  data: table.Row,
) -> Result(Schema, table.DataError) {
  use row <- try(table.match_fields(schema.table, data))
  // TODO: canonical form when we have default values for `table.Field`
  let primary_key_ids = schema.table.primary_key
  let primary_key =
    list.filter_map(
      row,
      fn(cell) {
        case list.contains(primary_key_ids, cell.field.id) {
          True -> Ok(cell.data)
          False -> Error(Nil)
        }
      },
    )
  let primary_key_bitstring =
    list.fold(
      primary_key,
      <<>>,
      fn(acc, pk) {
        let pk = data_type.to_bitstring(pk)
        <<acc:bit_string, pk:bit_string>>
      },
    )
  let row_data = list.map(row, fn(cell) { cell.data })
  let row_data_bitstring =
    list.fold(
      row_data,
      <<>>,
      fn(acc, data) {
        let data = data_type.to_bitstring(data)
        <<acc:bit_string, data:bit_string>>
      },
    )

  let key_hash = memtable.hash(primary_key_bitstring)
  let #(range_id, schema) = find_range(schema, key_hash)
  let assert Ok(range) = map.get(schema.memtable_ranges, range_id)
  let assert Some(memtable) = range.memtable
  let schema =
    Schema(..schema, value_index: index.update_active(schema.value_index))
  case memtable.get(memtable, primary_key_bitstring) {
    Ok(old_value) -> {
      // key is in the index, we have to replace it
      case
        store_value(
          schema,
          primary_key_bitstring,
          range_id,
          range,
          memtable,
          row_data_bitstring,
        )
      {
        Ok(schema) -> schema
        Error(CapacityExceeded) -> {
          let schema = split(schema, key_hash, range_id, range, memtable)
          let assert Ok(schema) = insert(schema, data)
          schema
        }
      }
      let assert Ok(schema) = delete_value(schema, old_value)
      Ok(schema)
    }
    Error(Nil) -> {
      // key isn't in the index yet, insert it as a new key
      case
        store_value(
          schema,
          primary_key_bitstring,
          range_id,
          range,
          memtable,
          row_data_bitstring,
        )
      {
        Ok(schema) -> Ok(schema)
        Error(CapacityExceeded) -> {
          let schema = split(schema, key_hash, range_id, range, memtable)
          let assert Ok(schema) = insert(schema, data)
          Ok(schema)
        }
      }
    }
  }
}

fn store_value(
  schema: Schema,
  key: BitString,
  range_id: Int,
  range: MemTableRange,
  memtable: MemTable,
  value_data: BitString,
) -> Result(Schema, memtable.Reason) {
  let assert Ok(vfile) = index.get_active(schema.value_index)
  case value.write(vfile, value_data) {
    Ok(#(vfile, value)) -> {
      use memtable <- try(memtable.set(memtable, key, value))
      let range = MemTableRange(..range, memtable: Some(memtable))
      Ok(
        Schema(
          ..schema,
          value_index: index.set(schema.value_index, vfile),
          memtable_ranges: map.insert(schema.memtable_ranges, range_id, range),
        ),
      )
    }
    Error(value.CapacityExceeded) -> {
      let assert Ok(vfile) =
        value.create(
          fs.join([schema.base_path, "value"]),
          schema.max_value_size,
        )
      Schema(..schema, value_index: index.set(schema.value_index, vfile))
      |> store_value(key, range_id, range, memtable, value_data)
    }
  }
}

fn delete_value(schema: Schema, value: Value) -> Result(Schema, Nil) {
  let assert Ok(vfile) = index.get(schema.value_index, value.file_id)
  let assert Ok(vfile) = value.delete(vfile, value)
  Ok(Schema(..schema, value_index: index.set(schema.value_index, vfile)))
}

fn split(
  schema: Schema,
  key_hash: Int,
  range_id: Int,
  range: MemTableRange,
  memtable: MemTable,
) -> Schema {
  let ranges =
    range.split(
      schema.memtable_ranges,
      schema.base_path,
      sstable.Key,
      key_hash,
      range_id,
      range,
      memtable,
    )
  Schema(..schema, memtable_ranges: ranges)
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
  let key_bitstring = data_type.to_bitstring(key)
  let key_hash = memtable.hash(key_bitstring)
  let #(range_id, schema) = find_range(schema, key_hash)
  let assert Ok(range) = map.get(schema.memtable_ranges, range_id)
  let assert Some(memtable) = range.memtable
  case memtable.get(memtable, key_bitstring) {
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
