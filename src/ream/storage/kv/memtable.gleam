import gleam/bit_string
import gleam/list
import gleam/map.{Map}
import gleam/option.{None}
import ream/storage/kv/value.{Value}

/// MemTable holds a sorted list of the latest written records.
///
/// Writes are duplicated to the WAL for recovery of the MemTable in the event of a restart.
///
/// MemTables have a max capacity and when that is reached, we flush the MemTable
/// to disk as a Table (Sorted String Table or SSTable).
///
/// Entries are stored in a Vector instead of a HashMap to support Scans.
pub type MemTable {
  MemTable(entries: Map(Int, MemTableEntry), size: Int, max_size: Int)
}

/// MemTableEntry is a single entry in the MemTable.
pub type MemTableEntry {
  MemTableEntry(key: String, value: Value)
}

pub type Reason {
  CapacityExceeded
}

/// The key hash size is 32-bit integer
pub const key_hash_size_bytes = 4

/// The key string size data is 16-bit integer
pub const key_size_bytes = 2

/// The file ID size is 128-bit integer corresponding to the UUID
/// of the file in binary format.
pub const file_id_size_bytes = 16

/// The file offset size is 32-bit integer.
pub const file_offset_size_bytes = 4

/// The payload size for the memtable entry, it is the sum of the
/// key hash size, file ID size, file offset size and key size.
pub const payload_size_bytes = 26

pub fn new(max_size: Int) -> MemTable {
  MemTable(entries: map.new(), size: 0, max_size: max_size)
}

pub fn from_entries(entries: Map(Int, MemTableEntry), max_size: Int) -> MemTable {
  let size = calculate_entries_size(entries)
  MemTable(entries: entries, size: size, max_size: max_size)
}

pub fn entry_to_bitstring(entry: MemTableEntry) -> BitString {
  let key_hash = hash(entry.key)
  let key_string = bit_string.from_string(entry.key)
  let key_size = bit_string.byte_size(key_string)
  let file_id = entry.value.file_id
  let file_offset = entry.value.offset
  <<
    key_hash:128,
    key_size:16,
    file_id:128,
    file_offset:32,
    key_string:bit_string,
  >>
}

pub fn bitstring_to_entry(bitstring: BitString) -> #(Int, MemTableEntry) {
  let <<
    key_hash:128,
    _key_size:16,
    file_id:128,
    file_offset:32,
    key_string:bit_string,
  >> = bitstring
  let assert Ok(key) = bit_string.to_string(key_string)
  let value =
    Value(data: None, deleted: False, file_id: file_id, offset: file_offset)
  #(key_hash, MemTableEntry(key: key, value: value))
}

pub fn contains(mem_table: MemTable, key: String) -> Bool {
  map.has_key(mem_table.entries, hash(key))
}

pub external fn hash(key: String) -> Int =
  "erlang" "phash2"

pub fn set(
  mem_table: MemTable,
  key: String,
  value: Value,
) -> Result(MemTable, Reason) {
  let key_hash = hash(key)
  case map.get(mem_table.entries, key_hash) {
    Error(Nil) -> {
      let entry = MemTableEntry(key, value)
      let entry_size = calculate_size(entry)
      let current_size = mem_table.size + entry_size
      case current_size > mem_table.max_size {
        True -> Error(CapacityExceeded)
        False ->
          Ok(
            MemTable(
              ..mem_table,
              entries: map.insert(mem_table.entries, key_hash, entry),
              size: current_size,
            ),
          )
      }
    }
    Ok(old_entry) -> {
      let old_entry_size = calculate_size(old_entry)
      let entry = MemTableEntry(..old_entry, value: value)
      let current_entry_size = calculate_size(entry)
      let mem_table_size = mem_table.size + current_entry_size - old_entry_size
      case mem_table_size > mem_table.max_size {
        True -> Error(CapacityExceeded)
        False ->
          Ok(
            MemTable(
              ..mem_table,
              entries: map.insert(mem_table.entries, key_hash, entry),
              size: mem_table_size,
            ),
          )
      }
    }
  }
}

pub fn delete(mem_table: MemTable, key: String) -> MemTable {
  let key_hash = hash(key)
  case map.get(mem_table.entries, key_hash) {
    Error(Nil) -> mem_table
    Ok(entry) -> {
      MemTable(
        ..mem_table,
        entries: map.delete(mem_table.entries, key_hash),
        size: mem_table.size - calculate_size(entry),
      )
    }
  }
}

pub fn get(mem_table: MemTable, key: String) -> Result(MemTableEntry, Nil) {
  map.get(mem_table.entries, hash(key))
}

fn search_pivot(entries: Map(Int, MemTableEntry)) -> Int {
  let keys = map.keys(entries)
  let entries_count = map.size(entries)
  let #(_, [pivot, ..]) = list.split(keys, at: entries_count / 2)
  pivot
}

pub fn split(mem_table: MemTable) -> #(MemTable, MemTable) {
  let pivot = search_pivot(mem_table.entries)
  let #(low_entries, high_entries) =
    mem_table.entries
    |> map.to_list()
    |> list.partition(fn(entry) { entry.0 < pivot })

  let low_entries = map.from_list(low_entries)

  let low =
    MemTable(
      ..mem_table,
      entries: low_entries,
      size: calculate_entries_size(low_entries),
    )

  let high_entries = map.from_list(high_entries)

  let high =
    MemTable(
      ..mem_table,
      entries: high_entries,
      size: calculate_entries_size(high_entries),
    )

  #(low, high)
}

fn calculate_entries_size(entries: Map(Int, MemTableEntry)) -> Int {
  map.fold(entries, 0, fn(acc, _key, entry) { acc + calculate_size(entry) })
}

fn calculate_size(entry: MemTableEntry) -> Int {
  bit_string.byte_size(entry_to_bitstring(entry))
}

pub fn get_bounds(mem_table: MemTable) -> #(Int, Int) {
  case map.to_list(mem_table.entries) {
    [] -> #(0, 0)
    [#(k, _), ..entries] -> {
      list.fold(
        entries,
        #(k, k),
        fn(acc: #(Int, Int), entry) {
          #(get_lower(acc.0, entry.0), get_higher(acc.1, entry.0))
        },
      )
    }
  }
}

fn get_lower(lower_bound: Int, key: Int) -> Int {
  case lower_bound {
    0 -> key
    lower_bound if key < lower_bound -> key
    lower_bound -> lower_bound
  }
}

fn get_higher(higher_bound: Int, key: Int) -> Int {
  case higher_bound {
    0 -> key
    higher_bound if key > higher_bound -> key
    higher_bound -> higher_bound
  }
}
