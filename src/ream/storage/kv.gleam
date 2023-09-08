//// The aggregators are storing their information based on a key and a value,
//// a document. This content is going to be updated and read more than
//// inserted as a new element. However, I think the best approach is a
//// copy-on-write strategy. If we need to modify the aggregator we insert a
//// new element inside of the files in the same way we do with the events and
//// then we update the index.
//// 
//// To avoid some race conditions that could arise reading the aggregator when
//// we are processing an update the process in charge of the modification
//// could lock the possible reading actions until the new element is updated
//// inside of the index. However, there are situations where eventual
//// consistency is ok. We could implement a configuration parameter:
//// 
//// - `aggregator.read_lock_when_writing` using a default value of `true`,
////   we encourage consistency.
//// 
//// > **Note**
//// > However, it's not going to lock the events and it's not going to lock
//// > other aggregators or projections.
//// 
//// The process file has the information about the elements it's storing and
//// if all of them are marked as deleted, then the file is removed. Based on
//// that the information of the aggregators could be generated again from the
//// events makes no sense to keep that information.
//// 
//// As another option, we could add a frequency to perform a data vacuum.
//// That's going to be the process to get the information still valid from the
//// older files where there is removed information and we put that information
//// as if that was called to be modified. It's going into the new files and
//// then the old file is removed. We could define:
//// 
//// - `aggregator.vacuum_factor` where we specify a number between 1 and 100
////   and if that percentage of elements is removed then the file is removed
////   and all of their elements to a new file. The default value is 100.
//// - `aggregator.vacuum_frequency` indicates the time in seconds when the
////   vacuum is triggered. The default value is 86400 (one day).

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
  let assert #(memtables_loaded, ranges) = range.load(path, max_memtable_size)

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
