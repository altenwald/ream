//// Schema is helping you to create structured data to be stored in the disk.
//// The schema is behaving as the most of the SQL-like databases around, it's
//// letting us configure the tables, their primary keys, and unique and
//// normal indexes.

import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Some}
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

fn check_table(
  fields: List(#(FieldId, table.Field)),
  primary_key_ids: List(FieldId),
) -> Result(Nil, table.DataError) {
  case primary_key_ids {
    [] -> Ok(Nil)
    [pk_id, ..pk_ids] -> {
      case list.key_find(fields, pk_id) {
        Ok(field) ->
          case field.nilable {
            True -> Error(table.PrimaryKeyCannotBeNull(field))
            False -> check_table(fields, pk_ids)
          }
        Error(Nil) -> Error(table.PrimaryKeyRefInvalid(pk_id))
      }
    }
  }
}

pub fn create(
  table: Table,
  max_memtable_size: Int,
  max_memtables_loaded: Int,
  max_value_size: Int,
  path: String,
) -> Result(Schema, table.DataError) {
  let check_data = list.map(table.fields, fn(field) { #(field.id, field) })
  use _ <- try(check_table(check_data, table.primary_key))
  let path = fs.join([path, "schema", table.name])
  let assert Ok(True) = fs.recursive_make_directory(path)

  // save the structure of the table
  let meta_file_path = fs.join([path, "meta"])
  let assert Ok(Nil) = table.flush(table, meta_file_path)

  // load memtables where the primary keys are stored
  let key_dir = fs.join([path, "key"])
  let assert Ok(True) = fs.recursive_make_directory(key_dir)
  let assert #(memtables_loaded, ranges) = range.load(path, max_memtable_size)

  // open the files where the values (rows) are stored
  let value_dir = fs.join([path, "value"])
  let value_index = index.load(value_dir, max_value_size)

  Ok(Schema(
    path,
    table,
    ranges,
    value_index,
    memtables_loaded,
    max_memtables_loaded,
    max_memtable_size,
    max_value_size,
  ))
}

pub fn open(
  table_name: String,
  max_memtable_size: Int,
  max_memtables_loaded: Int,
  max_value_size: Int,
  path: String,
) -> Schema {
  let path = fs.join([path, "schema", table_name])

  // load the structure of the table
  let meta_file_path = fs.join([path, "meta"])
  let assert Ok(table) = table.load(meta_file_path)

  // load memtables where the primary keys are stored
  let assert #(memtables_loaded, ranges) = range.load(path, max_memtable_size)

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

fn primary_key_match(
  primary_key_ids: List(FieldId),
  cells: List(table.DataSet),
  acc: List(DataType),
) -> Result(List(DataType), table.DataError) {
  case cells {
    [] -> Ok(list.reverse(acc))
    [cell, ..rest_cells] -> {
      case list.contains(primary_key_ids, cell.field.id) {
        True ->
          primary_key_match(primary_key_ids, rest_cells, [cell.data, ..acc])
        False -> primary_key_match(primary_key_ids, rest_cells, acc)
      }
    }
  }
}

fn to_bitstring(entries: List(DataType)) -> BitString {
  list.fold(
    entries,
    <<>>,
    fn(acc, entry) {
      let entry_bitstring = data_type.to_bitstring(entry)
      <<acc:bit_string, entry_bitstring:bit_string>>
    },
  )
}

pub fn insert(
  schema: Schema,
  data: table.Row,
) -> Result(Schema, table.DataError) {
  use cells <- try(table.match_fields(schema.table, data))
  use primary_key <- try(primary_key_match(schema.table.primary_key, cells, []))
  let primary_key_bitstring = to_bitstring(primary_key)
  let row_data = list.map(cells, fn(cell) { cell.data })
  let row_data_bitstring = to_bitstring(row_data)

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
        Ok(schema) -> {
          let assert Ok(schema) = delete_value(schema, old_value)
          Ok(schema)
        }
        Error(CapacityExceeded) -> {
          let schema = split(schema, key_hash, range_id, range, memtable)
          insert(schema, data)
        }
      }
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
          insert(schema, data)
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
) -> #(Result(List(List(DataType)), SchemaReason), Schema) {
  case operation, schema.table.primary_key {
    All, _ -> #(Ok(get_all(schema)), schema)
    Equal(Field(id1), Literal(key)), [id2] if id1 == id2 -> {
      case get(schema, key) {
        #(Ok(row), schema) -> #(Ok([row]), schema)
        #(Error(reason), schema) -> #(Error(reason), schema)
      }
    }
    Equal(Literal(key), Field(id1)), [id2] if id1 == id2 -> {
      case get(schema, key) {
        #(Ok(row), schema) -> #(Ok([row]), schema)
        #(Error(reason), schema) -> #(Error(reason), schema)
      }
    }
    _, _ -> #(Error(NotImplemented(operation)), schema)
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

fn get_all(schema: Schema) -> List(List(DataType)) {
  list.map(
    map.to_list(schema.memtable_ranges),
    fn(range) {
      let assert Ok(memtable) = case range {
        #(memtable_id, MemTableRange(memtable: None, ..)) -> {
          sstable.load(
            sstable.path(schema.base_path, sstable.Key, memtable_id),
            schema.max_memtable_size,
          )
        }
        #(_memtable_id, MemTableRange(memtable: Some(memtable), ..)) -> {
          Ok(memtable)
        }
      }
      list.map(
        memtable.get_all(memtable),
        fn(entry) {
          let assert Ok(entries) = case entry {
            #(_id, Value(data: Some(data), ..)) ->
              from_bitstring(schema.table.fields, data, [])
            #(_id, Value(file_id: file_id, offset: offset, ..)) -> {
              let assert Ok(vfile) = index.get(schema.value_index, file_id)
              let assert Ok(Value(data: Some(data), ..)) =
                value.read(vfile, offset)
              from_bitstring(schema.table.fields, data, [])
            }
          }
          entries
        },
      )
    },
  )
  |> list.concat()
}

fn get(
  schema: Schema,
  key: DataType,
) -> #(Result(List(DataType), SchemaReason), Schema) {
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
          from_bitstring(schema.table.fields, data, []),
          schema,
        )
        _ -> #(Error(NotFound), schema)
      }
    }
    Error(_err) -> #(Error(NotFound), schema)
  }
}

pub type SchemaReason {
  NotFound
  NotImplemented(Operation)
  UnexpectedData(BitString)
  MissingFields(List(table.Field))
  UnmatchField(table.Field, DataType)
}

fn from_bitstring(fields, data, acc) -> Result(List(DataType), SchemaReason) {
  case fields, data {
    [], <<>> -> Ok(list.reverse(acc))
    [], _ -> Error(UnexpectedData(data))
    _, <<>> -> Error(MissingFields(fields))
    [field, ..rest_fields], _ -> {
      case data_type.from_bitstring(data), field.field_type, field.nilable {
        #(data_type.Integer(i), rest_data), table.Integer, _ ->
          from_bitstring(rest_fields, rest_data, [data_type.Integer(i), ..acc])
        #(data_type.Float(f), rest_data), table.Float, _ ->
          from_bitstring(rest_fields, rest_data, [data_type.Float(f), ..acc])
        #(data_type.Decimal(d, b), rest_data), table.Decimal, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [data_type.Decimal(d, b), ..acc],
          )
        #(data_type.String(s), rest_data), table.String(_), _ ->
          from_bitstring(rest_fields, rest_data, [data_type.String(s), ..acc])
        #(data_type.BitString(b), rest_data), table.BitString(_), _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [data_type.BitString(b), ..acc],
          )
        #(data_type.Timestamp(t), rest_data), table.Timestamp, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [data_type.Timestamp(t), ..acc],
          )
        #(data_type.Null, rest_data), _, True ->
          from_bitstring(rest_fields, rest_data, [data_type.Null, ..acc])
        #(cell, _rest), _field_type, _field_nilable ->
          Error(UnmatchField(field, cell))
      }
    }
  }
}
