import gleam/erlang/file
import gleam/option.{None}
import ream/storage/file as fs
import ream/storage/memtable
import ream/storage/sstable
import ream/storage/value.{Value}

const base_path = "build/sstable_test/"

const max_size = 500

pub fn sstable_flush_test() {
  let path = fs.join([base_path, "values"])
  let _ = file.recursive_delete(path)

  let mem_table = memtable.new(max_size)

  let <<file_id:128>> = <<0:128>>
  let value1 = Value(0, False, None, file_id)
  let value2 = Value(1, False, None, file_id)
  let value3 = Value(2, False, None, file_id)
  let value4 = Value(3, False, None, file_id)

  let assert Ok(mem_table) = memtable.set(mem_table, <<"key1":utf8>>, value1)
  let assert Ok(mem_table) = memtable.set(mem_table, <<"key2":utf8>>, value2)
  let assert Ok(mem_table) = memtable.set(mem_table, <<"key3":utf8>>, value3)
  let assert Ok(mem_table) = memtable.set(mem_table, <<"key4":utf8>>, value4)

  let assert True = memtable.contains(mem_table, <<"key1":utf8>>)
  let assert True = memtable.contains(mem_table, <<"key2":utf8>>)
  let assert True = memtable.contains(mem_table, <<"key3":utf8>>)
  let assert True = memtable.contains(mem_table, <<"key4":utf8>>)

  let assert Ok(True) = sstable.flush(mem_table, fs.join([path, "1.sst"]))

  let assert Ok(mem_table_loaded) =
    sstable.load(fs.join([path, "1.sst"]), max_size)

  let assert Ok(value) = memtable.get(mem_table_loaded, <<"key1":utf8>>)
  let assert 0 = value.offset
  let assert Ok(value) = memtable.get(mem_table_loaded, <<"key2":utf8>>)
  let assert 1 = value.offset
  let assert Ok(value) = memtable.get(mem_table_loaded, <<"key3":utf8>>)
  let assert 2 = value.offset
  let assert Ok(value) = memtable.get(mem_table_loaded, <<"key4":utf8>>)
  let assert 3 = value.offset
}
