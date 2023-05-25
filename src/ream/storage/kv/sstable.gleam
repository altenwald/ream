import gleam/bit_string
import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/map.{Map}
import gleam/option.{None, Some}
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/kv/memtable.{MemTable, MemTableEntry}

pub const key_size_bits = 32

pub const value_size_bits = 16

pub const header_size = 6

pub fn flush(mem_table: MemTable, path: String) -> Result(Bool, file.Reason) {
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(path))
  let assert Ok(file) = fs.open(path, [fs.Write])
  map.filter(
    mem_table.entries,
    fn(key, entry) {
      // FIXME: https://github.com/gleam-lang/gleam/issues/2166
      let key_size_bits = key_size_bits
      let value_size_bits = value_size_bits
      // end FIXME
      let packed_value = case entry.value {
        Some(value) -> {
          let value_size = bit_string.byte_size(value)
          bit_string.concat([<<value_size:size(value_size_bits)>>, value])
        }
        None -> <<0:size(key_size_bits)>>
      }
      let key_content = <<key:size(key_size_bits)>>
      let content = bit_string.concat([key_content, packed_value])
      let assert Ok(_) = fs.write(file, content)
      False
    },
  )
  let assert Ok(_) = fs.close(file)
  Ok(True)
}

pub fn load(path: String, max_size: Int) -> Result(MemTable, file.Reason) {
  let assert Ok(file) = fs.open(path, [fs.Read])
  let assert Ok(entries) = read_entries(file, map.new())
  let assert Ok(_) = fs.close(file)
  Ok(memtable.from_entries(entries, max_size))
}

fn read_entries(
  file: Pid,
  entries: Map(Int, MemTableEntry),
) -> Result(Map(Int, MemTableEntry), file.Reason) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let key_size_bits = key_size_bits
  let value_size_bits = value_size_bits
  // end FIXME
  case fs.read(file, header_size) {
    read.Ok(<<key:size(key_size_bits), value_size:size(value_size_bits)>>) -> {
      let value = case value_size {
        0 -> None
        _ -> {
          let assert read.Ok(value) = fs.read(file, value_size)
          Some(value)
        }
      }
      let entries = map.insert(entries, key, MemTableEntry(value))
      read_entries(file, entries)
    }
    read.Eof -> Ok(entries)
    read.Error(reason) -> Error(reason)
  }
}
