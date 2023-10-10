//// Indeed, we have only schemas for projections and the rest of the systems
//// are only storing information about:
//// 
//// - `name` for the stream of events, collection of similar aggregators or
////   tables for projections.
//// - The `type` is one of the following: `stream``, `aggregator`, or
////   `projection`.
//// 
//// The files for these elements are going to be created under a directory
//// with that name: `type/name`; i.e. if we are creating the stream `users`
//// then the files for storing the events for that stream will be placed
//// under `stream/users`.
//// 
//// In the case of the projections, we are placing an extra file inside of
//// that directory called `schema`. The file contains a list of the fields
//// available for the projection. The content for each field is:
//// 
//// - `name` of the field.
//// - `type` of the filed. It could be one of these: `normal`, `index`, or
////   `unique`.
//// 
//// This way we have all of the information about the information we could
//// find about the projection and the information to generate the indexes.
//// 
//// Schema is helping you to create structured data to be stored in the disk.
//// The schema is behaving as the most of the SQL-like databases around, it's
//// letting us configure the tables, their primary keys, and unique and
//// normal indexes.
//// 
//// The projections have indexes a bit more complex and the requests could
//// filter the information to be retrieved based on an expression provided.
//// That's because each file is going to process its indexes. This is great
//// because we can parallelize the requests for reading and writing but it
//// makes a bit more difficult the unique indexes.
//// 
//// About the indexes, we have:
//// 
//// - Unique indexes are keeping a record per index across all of the files.
//// - Indexes are keeping many records per index. There will be one index
////   per file.
//// 
//// The only problem is about writing if we have defined unique keys. This
//// is going to have a simplified version out of the files for assigning the
//// unique index or refusing it, we are going to use a Cuckoo filter to ensure
//// the index wasn't inside of any of the files.
//// 
//// The storage of the projections is going to take place as the aggregations
//// and in the same way, we will have the vacuum of the information with the
//// following parameters:
//// 
//// - `projector.vacuum_factor` where we specify a number between 1 and 100
////   and if that percentage of elements are removed then the file is removed
////   and all of their elements to a new file. The default value is 100.
//// - `projector.vacuum_frequency` indicates the time in seconds when the
////   vacuum is triggered. The default value is 86400 (one day).

import gleam/bit_string
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Some}
import gleam/order
import gleam/regex
import gleam/result.{try}
import gleam/string
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
  Data(DataOperation)
  Array(List(DataOperation))
  Contains(Operation, Operation)
  In(Operation, List(DataOperation))
  Equal(Operation, Operation)
  NotEqual(Operation, Operation)
  LesserThan(Operation, Operation)
  GreaterThan(Operation, Operation)
  LesserOrEqualThan(Operation, Operation)
  GreaterOrEqualThan(Operation, Operation)
  Regex(Operation, Operation)
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

// TODO this should retrieve a cursor, a list of values and offsets
//      to be retrieved from the value files, or place the result
//      inside a temporal storage for retrieving that in a secure
//      way without blocking.
pub fn find(
  schema: Schema,
  operation: Operation,
) -> #(Result(List(List(DataType)), SchemaReason), Schema) {
  case operation, schema.table.primary_key {
    Equal(Data(Field(id1)), Data(Literal(key))), [id2] if id1 == id2 -> {
      case get(schema, key) {
        #(Ok(row), schema) -> #(Ok([row]), schema)
        #(Error(reason), schema) -> #(Error(reason), schema)
      }
    }
    Equal(Data(Literal(key)), Data(Field(id1))), [id2] if id1 == id2 -> {
      case get(schema, key) {
        #(Ok(row), schema) -> #(Ok([row]), schema)
        #(Error(reason), schema) -> #(Error(reason), schema)
      }
    }
    _, _ -> {
      let filter = to_filter(schema, operation)
      #(Ok(get_all(schema, filter)), schema)
    }
  }
}

pub type OperationFn =
  fn(table.Row) -> Bool

pub type DataOperationFn =
  fn(table.Row) -> DataType

fn to_filter_data(
  table: Table,
  data_operation: DataOperation,
) -> DataOperationFn {
  case data_operation {
    Field(field_id) -> fn(dataset: table.Row) {
      let assert Ok(field) = table.find_field(table, field_id)
      let assert Ok(data) = list.key_find(dataset, field.name)
      data
    }
    Literal(data) -> fn(_) { data }
  }
}

pub type DataListOperationFn =
  fn(table.Row) -> List(DataType)

fn to_filter_data_list(
  table: Table,
  data_operations: List(DataOperation),
) -> DataListOperationFn {
  fn(dataset: table.Row) {
    list.map(
      data_operations,
      fn(data_operation) {
        let f = to_filter_data(table, data_operation)
        f(dataset)
      },
    )
  }
}

fn to_filter(schema: Schema, operation: Operation) -> OperationFn {
  case operation {
    Data(Literal(literal)) -> fn(_) { data_type.to_bool(literal) }
    Data(Field(field_id)) -> fn(row) {
      let data = to_filter_data(schema.table, Field(field_id))
      data_type.to_bool(data(row))
    }
    All -> fn(_) { True }
    And(a, b) -> fn(row) {
      let left = to_filter(schema, a)
      let right = to_filter(schema, b)
      left(row) && right(row)
    }
    Array(_elements) -> fn(_) { True }
    Equal(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) == order.Eq
    }
    NotEqual(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) != order.Eq
    }
    GreaterOrEqualThan(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) != order.Lt
    }
    GreaterThan(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) == order.Gt
    }
    LesserOrEqualThan(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) != order.Gt
    }
    LesserThan(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      data_type.compare(left(row), right(row)) == order.Lt
    }
    Contains(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      contains(left(row), right(row))
    }
    In(Data(a), b) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data_list(schema.table, b)
      in(left(row), right(row))
    }
    Not(a) -> fn(row) {
      let unary = to_filter(schema, a)
      !unary(row)
    }
    Or(a, b) -> fn(row) {
      let left = to_filter(schema, a)
      let right = to_filter(schema, b)
      left(row) || right(row)
    }
    Regex(Data(a), Data(b)) -> fn(row) {
      let left = to_filter_data(schema.table, a)
      let right = to_filter_data(schema.table, b)
      regex(left(row), right(row))
    }
  }
}

fn regex(left: DataType, right: DataType) -> Bool {
  case left, right {
    data_type.String(l), data_type.String(r) -> {
      case regex.from_string(l) {
        Ok(re) -> regex.check(re, r)
        // TODO should we trigger an error here instead?
        _ -> False
      }
    }
    // TODO should we trigger an error here instead?
    _, _ -> False
  }
}

fn contains(left: DataType, right: DataType) -> Bool {
  case left, right {
    data_type.String(l), data_type.String(r) -> string.contains(l, r)
    data_type.BitString(l), _ -> {
      let assert Ok(l) = bit_string.to_string(l)
      contains(data_type.String(l), right)
    }
    _, data_type.BitString(r) -> {
      let assert Ok(r) = bit_string.to_string(r)
      contains(left, data_type.String(r))
    }
    // TODO should we trigger an error here instead?
    _, _ -> False
  }
}

fn in(needle: DataType, hay: List(DataType)) -> Bool {
  list.contains(hay, needle)
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

fn get_all(schema: Schema, filter: OperationFn) -> List(List(DataType)) {
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
      list.filter_map(
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
          case filter(entries) {
            True -> Ok(list.map(entries, fn(entry) { entry.1 }))
            False -> Error(Nil)
          }
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
        Ok(Value(deleted: False, file_id: _, offset: _, data: Some(data))) -> {
          let assert Ok(entries) = from_bitstring(schema.table.fields, data, [])
          #(Ok(list.map(entries, fn(entry) { entry.1 })), schema)
        }
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

fn from_bitstring(
  fields: List(table.Field),
  data: BitString,
  acc: table.Row,
) -> Result(table.Row, SchemaReason) {
  case fields, data {
    [], <<>> -> Ok(list.reverse(acc))
    [], _ -> Error(UnexpectedData(data))
    _, <<>> -> Error(MissingFields(fields))
    [field, ..rest_fields], _ -> {
      case data_type.from_bitstring(data), field.field_type, field.nilable {
        #(data_type.Integer(i), rest_data), table.Integer, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.Integer(i)), ..acc],
          )
        #(data_type.Float(f), rest_data), table.Float, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.Float(f)), ..acc],
          )
        #(data_type.Decimal(d, b), rest_data), table.Decimal, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.Decimal(d, b)), ..acc],
          )
        #(data_type.String(s), rest_data), table.String(_), _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.String(s)), ..acc],
          )
        #(data_type.BitString(b), rest_data), table.BitString(_), _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.BitString(b)), ..acc],
          )
        #(data_type.Timestamp(t), rest_data), table.Timestamp, _ ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.Timestamp(t)), ..acc],
          )
        #(data_type.Null, rest_data), _, True ->
          from_bitstring(
            rest_fields,
            rest_data,
            [#(field.name, data_type.Null), ..acc],
          )
        #(cell, _rest), _field_type, _field_nilable ->
          Error(UnmatchField(field, cell))
      }
    }
  }
}
