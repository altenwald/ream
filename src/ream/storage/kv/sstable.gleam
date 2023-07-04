import gleam/erlang/file
import gleam/erlang/process.{Subject}
import gleam/map.{Map}
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/kv/memtable.{MemTable, MemTableEntry}

pub const key_hash_size_bits = 128

pub const key_size_bits = 16

pub const file_id_size_bits = 128

pub const offset_size_bits = 32

const header_size = 38

pub fn flush(mem_table: MemTable, path: String) -> Result(Bool, file.Reason) {
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(path))
  let assert Ok(file) = fs.open(path, [fs.Write])
  // TODO maybe suggest the inclusion of `map.each/2` for gleam/stdlib
  map.filter(
    mem_table.entries,
    fn(_key, entry) {
      let content = memtable.entry_to_bitstring(entry)
      let assert Ok(_) = fs.write(file, fs.Cur(0), content)
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
  file: Subject(fs.Message),
  entries: Map(Int, MemTableEntry),
) -> Result(Map(Int, MemTableEntry), file.Reason) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let key_hash_size_bits = key_hash_size_bits
  let key_size_bits = key_size_bits
  let file_id_size_bits = file_id_size_bits
  let offset_size_bits = offset_size_bits
  // end FIXME
  case fs.read(file, fs.Cur(0), header_size) {
    read.Ok(<<
      key_hash:size(key_hash_size_bits),
      key_size:size(key_size_bits),
      file_id:size(file_id_size_bits),
      offset:size(offset_size_bits),
    >>) -> {
      let assert read.Ok(key_string) = fs.read(file, fs.Cur(0), key_size)
      let #(_key, entry) =
        memtable.bitstring_to_entry(<<
          key_hash:size(key_hash_size_bits),
          key_size:size(key_size_bits),
          file_id:size(file_id_size_bits),
          offset:size(offset_size_bits),
          key_string:bit_string,
        >>)
      let entries = map.insert(entries, key_hash, entry)
      read_entries(file, entries)
    }
    read.Eof -> Ok(entries)
    read.Error(reason) -> Error(reason)
  }
}
