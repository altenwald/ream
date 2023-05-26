import gleam/map
import gleam/option.{None}
import ream/storage/kv/memtable.{CapacityExceeded, MemTableEntry}
import ream/storage/kv/value.{Value}

pub fn memtable_happy_path_test() {
  let mem_table = memtable.new(500)

  let <<file_id:128>> = <<0:128>>

  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value1)
  let assert Ok(mem_table) = memtable.set(mem_table, "key2", value2)
  let assert Ok(mem_table) = memtable.set(mem_table, "key3", value3)

  let assert True = memtable.contains(mem_table, "key1")
  let assert True = memtable.contains(mem_table, "key2")
  let assert True = memtable.contains(mem_table, "key3")

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 0 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key2")
  let assert 1 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key3")
  let assert 2 = entry.value.offset
}

pub fn memtable_from_entries_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)

  let entries =
    [
      #(memtable.hash("key1"), MemTableEntry("key1", value1)),
      #(memtable.hash("key2"), MemTableEntry("key2", value2)),
      #(memtable.hash("key3"), MemTableEntry("key3", value3)),
    ]
    |> map.from_list()

  let mem_table = memtable.from_entries(entries, 500)

  let assert True = memtable.contains(mem_table, "key1")
  let assert True = memtable.contains(mem_table, "key2")
  let assert True = memtable.contains(mem_table, "key3")

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 0 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key2")
  let assert 1 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key3")
  let assert 2 = entry.value.offset
}

pub fn memtable_set_and_update_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)
  let value4 = Value(3, False, None, file_id)

  let entries =
    [
      #(memtable.hash("key1"), MemTableEntry("key1", value1)),
      #(memtable.hash("key2"), MemTableEntry("key2", value2)),
      #(memtable.hash("key3"), MemTableEntry("key3", value3)),
      #(memtable.hash("key4"), MemTableEntry("key4", value4)),
    ]
    |> map.from_list()

  let mem_table = memtable.from_entries(entries, 500)

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 0 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key2")
  let assert 1 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key3")
  let assert 2 = entry.value.offset

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value4)

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 3 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key2")
  let assert 1 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key3")
  let assert 2 = entry.value.offset
}

pub fn memtable_exceeded_capacity_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)

  let mem_table = memtable.new(50)
  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value1)

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 0 = entry.value.offset

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value2)

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 1 = entry.value.offset

  let assert Error(CapacityExceeded) = memtable.set(mem_table, "key2", value2)
}

pub fn memtable_delete_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)

  let mem_table = memtable.new(500)

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value1)
  let assert Ok(mem_table) = memtable.set(mem_table, "key2", value2)
  let assert Ok(mem_table) = memtable.set(mem_table, "key3", value3)

  let assert True = memtable.contains(mem_table, "key1")
  let assert True = memtable.contains(mem_table, "key2")
  let assert True = memtable.contains(mem_table, "key3")

  let assert Ok(entry) = memtable.get(mem_table, "key1")
  let assert 0 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key2")
  let assert 1 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table, "key3")
  let assert 2 = entry.value.offset

  let mem_table = memtable.delete(mem_table, "key1")
  let mem_table = memtable.delete(mem_table, "key1")
  let assert False = memtable.contains(mem_table, "key1")
  let assert Error(Nil) = memtable.get(mem_table, "key1")
}

pub fn memtable_split_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)
  let value4 = Value(3, False, None, file_id)

  let mem_table = memtable.new(500)

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value1)
  let assert Ok(mem_table) = memtable.set(mem_table, "key2", value2)
  let assert Ok(mem_table) = memtable.set(mem_table, "key3", value3)
  let assert Ok(mem_table) = memtable.set(mem_table, "key4", value4)

  let assert True = memtable.contains(mem_table, "key1")
  let assert True = memtable.contains(mem_table, "key2")
  let assert True = memtable.contains(mem_table, "key3")
  let assert True = memtable.contains(mem_table, "key4")

  let assert #(mem_table1, mem_table2) = memtable.split(mem_table)

  let assert Ok(entry) = memtable.get(mem_table2, "key1")
  let assert 0 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table2, "key2")
  let assert 1 = entry.value.offset
  let assert Error(Nil) = memtable.get(mem_table2, "key3")
  let assert Error(Nil) = memtable.get(mem_table2, "key4")

  let assert Error(Nil) = memtable.get(mem_table1, "key1")
  let assert Error(Nil) = memtable.get(mem_table1, "key2")
  let assert Ok(entry) = memtable.get(mem_table1, "key3")
  let assert 2 = entry.value.offset
  let assert Ok(entry) = memtable.get(mem_table1, "key4")
  let assert 3 = entry.value.offset
}

pub fn get_bounds_test() {
  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)

  let mem_table = memtable.new(500)

  let assert Ok(mem_table) = memtable.set(mem_table, "key1", value1)
  let assert Ok(mem_table) = memtable.set(mem_table, "key2", value2)
  let assert Ok(mem_table) = memtable.set(mem_table, "key3", value3)

  let assert #(10_009_508, 126_902_492) = memtable.get_bounds(mem_table)
}
