import gleam/bit_string
import gleam/map
import gleam/option.{Some}
import ream/storage/kv/memtable.{CapacityExceeded, MemTableEntry}

pub fn memtable_happy_path_test() {
  let mem_table = memtable.new(500)

  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")

  let assert Ok(mem_table) = memtable.set(mem_table, 1, value1)
  let assert Ok(mem_table) = memtable.set(mem_table, 10, value2)
  let assert Ok(mem_table) = memtable.set(mem_table, 100, value3)

  let assert True = memtable.contains(mem_table, 1)
  let assert True = memtable.contains(mem_table, 10)
  let assert True = memtable.contains(mem_table, 100)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table, 10)
  let assert True = entry.value == Some(value2)
  let assert Ok(entry) = memtable.get(mem_table, 100)
  let assert True = entry.value == Some(value3)
}

pub fn memtable_from_entries_test() {
  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")

  let entries =
    [
      #(1, MemTableEntry(Some(value1))),
      #(10, MemTableEntry(Some(value2))),
      #(100, MemTableEntry(Some(value3))),
    ]
    |> map.from_list()

  let mem_table = memtable.from_entries(entries, 500)

  let assert True = memtable.contains(mem_table, 1)
  let assert True = memtable.contains(mem_table, 10)
  let assert True = memtable.contains(mem_table, 100)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table, 10)
  let assert True = entry.value == Some(value2)
  let assert Ok(entry) = memtable.get(mem_table, 100)
  let assert True = entry.value == Some(value3)
}

pub fn memtable_set_and_update_test() {
  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")
  let value4 = bit_string.from_string("value4")

  let entries =
    [
      #(1, MemTableEntry(Some(value1))),
      #(10, MemTableEntry(Some(value2))),
      #(100, MemTableEntry(Some(value3))),
    ]
    |> map.from_list()

  let mem_table = memtable.from_entries(entries, 500)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table, 10)
  let assert True = entry.value == Some(value2)
  let assert Ok(entry) = memtable.get(mem_table, 100)
  let assert True = entry.value == Some(value3)

  let assert Ok(mem_table) = memtable.set(mem_table, 10, value4)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table, 10)
  let assert True = entry.value == Some(value4)
  let assert Ok(entry) = memtable.get(mem_table, 100)
  let assert True = entry.value == Some(value3)
}

pub fn memtable_exceeded_capacity_test() {
  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value_too_big = bit_string.from_string("this value is too big")

  let mem_table = memtable.new(15)
  let assert Ok(mem_table) = memtable.set(mem_table, 1, value1)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)

  let assert Ok(mem_table) = memtable.set(mem_table, 1, value2)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value2)

  let assert Error(CapacityExceeded) = memtable.set(mem_table, 2, value_too_big)
  let assert Error(CapacityExceeded) = memtable.set(mem_table, 1, value_too_big)
}

pub fn memtable_delete_test() {
  let mem_table = memtable.new(500)

  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")

  let assert Ok(mem_table) = memtable.set(mem_table, 1, value1)
  let assert Ok(mem_table) = memtable.set(mem_table, 10, value2)
  let assert Ok(mem_table) = memtable.set(mem_table, 100, value3)

  let assert True = memtable.contains(mem_table, 1)
  let assert True = memtable.contains(mem_table, 10)
  let assert True = memtable.contains(mem_table, 100)

  let assert Ok(entry) = memtable.get(mem_table, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table, 10)
  let assert True = entry.value == Some(value2)
  let assert Ok(entry) = memtable.get(mem_table, 100)
  let assert True = entry.value == Some(value3)

  let mem_table = memtable.delete(mem_table, 1)
  let mem_table = memtable.delete(mem_table, 1)
  let assert False = memtable.contains(mem_table, 1)
  let assert Error(Nil) = memtable.get(mem_table, 1)
}

pub fn memtable_split_test() {
  let mem_table = memtable.new(500)

  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")
  let value4 = bit_string.from_string("value4")

  let assert Ok(mem_table) = memtable.set(mem_table, 1, value1)
  let assert Ok(mem_table) = memtable.set(mem_table, 10, value2)
  let assert Ok(mem_table) = memtable.set(mem_table, 100, value3)
  let assert Ok(mem_table) = memtable.set(mem_table, 1000, value4)

  let assert True = memtable.contains(mem_table, 1)
  let assert True = memtable.contains(mem_table, 10)
  let assert True = memtable.contains(mem_table, 100)
  let assert True = memtable.contains(mem_table, 1000)

  let assert #(mem_table1, mem_table2) = memtable.split(mem_table, 100)

  let assert Ok(entry) = memtable.get(mem_table1, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table1, 10)
  let assert True = entry.value == Some(value2)
  let assert Error(Nil) = memtable.get(mem_table1, 100)
  let assert Error(Nil) = memtable.get(mem_table1, 1000)

  let assert Error(Nil) = memtable.get(mem_table2, 1)
  let assert Error(Nil) = memtable.get(mem_table2, 10)
  let assert Ok(entry) = memtable.get(mem_table2, 100)
  let assert True = entry.value == Some(value3)
  let assert Ok(entry) = memtable.get(mem_table2, 1000)
  let assert True = entry.value == Some(value4)
}

pub fn get_bounds_test() {
  let mem_table = memtable.new(500)

  let value1 = bit_string.from_string("value1")
  let value2 = bit_string.from_string("value2")
  let value3 = bit_string.from_string("value3")

  let assert Ok(mem_table) = memtable.set(mem_table, 1, value1)
  let assert Ok(mem_table) = memtable.set(mem_table, 10, value2)
  let assert Ok(mem_table) = memtable.set(mem_table, 100, value3)

  let assert #(1, 100) = memtable.get_bounds(mem_table)
}
