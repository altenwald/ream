import gleam/bit_string
import gleam/erlang/file
import gleam/option.{Some}
import ream/storage/file as fs
import ream/storage/kv/memtable
import ream/storage/kv/sstable

const base_path = "build/sstable_test/"

const max_size = 500

pub fn sstable_flush_test() {
  let path = fs.join([base_path, "values"])
  let _ = file.recursive_delete(path)

  let mem_table = memtable.new(max_size)

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

  let assert Ok(True) = sstable.flush(mem_table, fs.join([path, "1.sst"]))

  let assert Ok(mem_table_loaded) =
    sstable.load(fs.join([path, "1.sst"]), max_size)

  let assert Ok(entry) = memtable.get(mem_table_loaded, 1)
  let assert True = entry.value == Some(value1)
  let assert Ok(entry) = memtable.get(mem_table_loaded, 10)
  let assert True = entry.value == Some(value2)
  let assert Ok(entry) = memtable.get(mem_table_loaded, 100)
  let assert True = entry.value == Some(value3)
}
