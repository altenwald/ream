import gleam/bit_string
import gleam/map.{Map}
import gleam/option.{None, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/memtable.{CapacityExceeded, MemTable}
import ream/storage/memtable/range.{MemTableRange}
import ream/storage/sstable
import ream/storage/value.{Value}
import ream/storage/value/index.{ValueIndex}

pub type KV {
  KV(
    base_path: String,
    name: String,
    memtable_ranges: Map(Int, MemTableRange),
    value_index: ValueIndex,
    memtables_loaded: Int,
    max_memtables_loaded: Int,
    max_memtable_size: Int,
    max_value_size: Int,
  )
}

pub type KVInfo {
  KVInfo(
    base_path: String,
    name: String,
    values: Int,
    values_size_bytes: Int,
    memtables_total: Int,
    memtables_loaded: Int,
    memtables_loaded_size_bytes: Int,
    max_memtables_loaded: Int,
    max_memtable_size: Int,
    max_value_size: Int,
  )
}

pub fn open(
  path: String,
  name: String,
  max_memtables_loaded: Int,
  max_memtable_size: Int,
  max_value_size: Int,
) -> KV {
  let path = fs.join([path, "kv", name])

  let key_dir = fs.join([path, "key"])
  let assert Ok(True) = fs.recursive_make_directory(key_dir)
  let assert #(memtables_loaded, ranges) =
    range.load(key_dir, max_memtable_size)

  let value_dir = fs.join([path, "value"])
  let value_index = index.load(value_dir, max_value_size)

  KV(
    path,
    name,
    ranges,
    value_index,
    memtables_loaded,
    max_memtables_loaded,
    max_memtable_size,
    max_value_size,
  )
}

fn find_range(kv: KV, key_hash: Int, max_size: Int) -> #(Int, KV) {
  let assert #(range_id, ranges, loaded) =
    range.find(
      kv.base_path,
      kv.memtable_ranges,
      kv.memtables_loaded,
      kv.max_memtables_loaded,
      sstable.Key,
      key_hash,
      max_size,
    )

  #(range_id, KV(..kv, memtable_ranges: ranges, memtables_loaded: loaded))
}

pub fn close(kv: KV) -> Result(Nil, Nil) {
  let assert Ok(_) = flush(kv)
  index.unload(kv.value_index)
}

pub fn flush(kv: KV) -> Result(Nil, Nil) {
  let key_dir = fs.join([kv.base_path, "key"])
  let assert Ok(True) = fs.recursive_make_directory(key_dir)
  let assert Ok(_) = range.flush(kv.base_path, sstable.Key, kv.memtable_ranges)

  let value_index_dir = fs.join([kv.base_path, "value"])
  let assert Ok(True) = fs.recursive_make_directory(value_index_dir)
  let assert Ok(_) = index.flush(kv.value_index)

  Ok(Nil)
}

pub fn get(kv: KV, key: String) -> #(Result(BitString, Nil), KV) {
  let key_bitstring = bit_string.from_string(key)
  let key_hash = memtable.hash(key_bitstring)
  let #(range_id, kv) = find_range(kv, key_hash, kv.max_memtable_size)
  let assert Ok(range) = map.get(kv.memtable_ranges, range_id)
  let assert Some(memtable) = range.memtable
  case memtable.get(memtable, key_bitstring) {
    Ok(value) -> {
      let assert Ok(vfile) = index.get(kv.value_index, value.file_id)
      case value.read(vfile, value.offset) {
        Ok(Value(deleted: False, file_id: _, offset: _, data: Some(data))) -> #(
          Ok(data),
          kv,
        )
        _ -> #(Error(Nil), kv)
      }
    }
    Error(_err) -> #(Error(Nil), kv)
  }
}

pub fn set(kv: KV, key: String, value: BitString) -> KV {
  let key_bitstring = bit_string.from_string(key)
  let key_hash = memtable.hash(key_bitstring)
  let #(range_id, kv) = find_range(kv, key_hash, kv.max_memtable_size)
  let assert Ok(range) = map.get(kv.memtable_ranges, range_id)
  let assert Some(memtable) = range.memtable
  let kv = KV(..kv, value_index: index.update_active(kv.value_index))
  case memtable.get(memtable, key_bitstring) {
    Ok(old_value) -> {
      // key is in the index, we have to replace it
      case store_value(kv, key_bitstring, range_id, range, memtable, value) {
        Ok(kv) -> kv
        Error(CapacityExceeded) -> {
          let kv = split(kv, key_hash, range_id, range, memtable)
          set(kv, key, value)
        }
      }
      let assert Ok(kv) = delete_value(kv, old_value)
      kv
    }
    Error(Nil) -> {
      // key isn't in the index yet, insert it as a new key
      case store_value(kv, key_bitstring, range_id, range, memtable, value) {
        Ok(kv) -> kv
        Error(CapacityExceeded) -> {
          let kv = split(kv, key_hash, range_id, range, memtable)
          set(kv, key, value)
        }
      }
    }
  }
}

fn split(
  kv: KV,
  key_hash: Int,
  range_id: Int,
  range: MemTableRange,
  memtable: MemTable,
) -> KV {
  let ranges =
    range.split(
      kv.memtable_ranges,
      kv.base_path,
      sstable.Key,
      key_hash,
      range_id,
      range,
      memtable,
    )
  KV(..kv, memtable_ranges: ranges)
}

fn store_value(
  kv: KV,
  key: BitString,
  range_id: Int,
  range: MemTableRange,
  memtable: MemTable,
  value_data: BitString,
) -> Result(KV, memtable.Reason) {
  let assert Ok(vfile) = index.get_active(kv.value_index)
  case value.write(vfile, value_data) {
    Ok(#(vfile, value)) -> {
      use memtable <- try(memtable.set(memtable, key, value))
      let range = MemTableRange(..range, memtable: Some(memtable))
      Ok(
        KV(
          ..kv,
          value_index: index.set(kv.value_index, vfile),
          memtable_ranges: map.insert(kv.memtable_ranges, range_id, range),
        ),
      )
    }
    Error(value.CapacityExceeded) -> {
      let assert Ok(vfile) =
        value.create(fs.join([kv.base_path, "value"]), kv.max_value_size)
      KV(..kv, value_index: index.set(kv.value_index, vfile))
      |> store_value(key, range_id, range, memtable, value_data)
    }
  }
}

fn delete_value(kv: KV, value: Value) -> Result(KV, Nil) {
  let assert Ok(vfile) = index.get(kv.value_index, value.file_id)
  let assert Ok(vfile) = value.delete(vfile, value)
  Ok(KV(..kv, value_index: index.set(kv.value_index, vfile)))
}

pub fn info(kv: KV) -> KVInfo {
  KVInfo(
    base_path: kv.base_path,
    name: kv.name,
    values: index.size(kv.value_index),
    values_size_bytes: index.byte_size(kv.value_index),
    memtables_total: map.size(kv.memtable_ranges),
    memtables_loaded: kv.memtables_loaded,
    memtables_loaded_size_bytes: map.fold(
      kv.memtable_ranges,
      0,
      fn(acc, _key, memtable_range) {
        case memtable_range.memtable {
          Some(memtable) -> acc + memtable.size
          None -> acc
        }
      },
    ),
    max_memtables_loaded: kv.max_memtables_loaded,
    max_memtable_size: kv.max_memtable_size,
    max_value_size: kv.max_value_size,
  )
}
