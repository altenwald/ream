import gleam/bit_string
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Option, Some}

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
  MemTableEntry(value: Option(BitString))
}

pub type Reason {
  CapacityExceeded
}

/// The key size is 32-bit integer
pub const key_size_bytes = 4

/// The value size of value is 16-bit integer
pub const size_for_value_size_bytes = 2

pub fn new(max_size: Int) -> MemTable {
  MemTable(entries: map.new(), size: 0, max_size: max_size)
}

pub fn from_entries(entries: Map(Int, MemTableEntry), max_size: Int) -> MemTable {
  let size = calculate_entries_size(entries)
  MemTable(entries: entries, size: size, max_size: max_size)
}

pub fn contains(mem_table: MemTable, key: Int) -> Bool {
  map.has_key(mem_table.entries, key)
}

pub fn set(
  mem_table: MemTable,
  key: Int,
  value: BitString,
) -> Result(MemTable, Reason) {
  case map.get(mem_table.entries, key) {
    Error(Nil) -> {
      let entry = MemTableEntry(value: Some(value))
      let entry_size =
        bit_string.byte_size(value) + key_size_bytes + size_for_value_size_bytes
      let current_size = mem_table.size + entry_size
      case current_size > mem_table.max_size {
        True -> Error(CapacityExceeded)
        False ->
          Ok(
            MemTable(
              ..mem_table,
              entries: map.insert(mem_table.entries, key, entry),
              size: current_size,
            ),
          )
      }
    }
    Ok(MemTableEntry(old_value)) -> {
      let old_value_size = case old_value {
        None -> 0
        Some(v) -> bit_string.byte_size(v)
      }
      let entry = MemTableEntry(value: Some(value))
      let current_size =
        mem_table.size + bit_string.byte_size(value) - old_value_size
      case current_size > mem_table.max_size {
        True -> Error(CapacityExceeded)
        False ->
          Ok(
            MemTable(
              ..mem_table,
              entries: map.insert(mem_table.entries, key, entry),
              size: mem_table.size + bit_string.byte_size(value) - old_value_size,
            ),
          )
      }
    }
  }
}

pub fn delete(mem_table: MemTable, key: Int) -> MemTable {
  case map.get(mem_table.entries, key) {
    Error(Nil) -> mem_table
    Ok(MemTableEntry(None)) -> {
      MemTable(..mem_table, entries: map.delete(mem_table.entries, key))
    }
    Ok(MemTableEntry(Some(value))) -> {
      MemTable(
        ..mem_table,
        entries: map.delete(mem_table.entries, key),
        size: mem_table.size - bit_string.byte_size(value) - key_size_bytes - size_for_value_size_bytes,
      )
    }
  }
}

pub fn get(mem_table: MemTable, key: Int) -> Result(MemTableEntry, Nil) {
  map.get(mem_table.entries, key)
}

pub fn split(mem_table: MemTable, pivot: Int) -> #(MemTable, MemTable) {
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
  case entry.value {
    None -> 0
    Some(value) ->
      bit_string.byte_size(value) + key_size_bytes + size_for_value_size_bytes
  }
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
