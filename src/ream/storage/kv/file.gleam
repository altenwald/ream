//// Store the information of the values for kv.
////
//// The events stored in the file are in the following format:
//// - 4 bytes: the size of the event
//// - n bytes: the event

import gleam/bit_string
import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/option.{Option, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/file/read
import ream/uuid

pub const value_size_bits = 32

pub type Value {
  Value(offset: Int, deleted: Bool, data: Option(BitString), file_id: Int)
}

/// The information for the kv file. The fields are:
/// - `id`: the id of the file. It's intended to be a UUID but it's stored as an Int.
/// - `handler`: the file handler to read and write values.
/// - `size`: the size of the file.
/// - `file_path`: the path of the file.
pub type KVFile {
  KVFile(id: Int, handler: Pid, size: Int, file_path: String)
}

/// Create a new kv file. It creates a new file with a random UUID as the
/// path for finding the file.
///
/// For example, if the UUID is `f81d4fae-7dec-11d0-a765-00a0c91e6bf6`, the
/// file will be created in the following path:
/// `base_path/f81d4fae/7dec/11d0/a765/00a0c91e6bf6`.
pub fn create(base_path: String) -> Result(KVFile, file.Reason) {
  let <<file_id:128>> = uuid.generate_v4()
  let file_name = get_file_name(base_path, file_id)
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
  use file_pid <- try(fs.open(file_name, [fs.Read, fs.Append]))
  Ok(KVFile(file_id, file_pid, 0, file_name))
}

fn get_file_name(base_path: String, file_id: Int) -> String {
  fs.join([base_path, ..uuid.parts(<<file_id:128>>)])
}

/// Open a kv file. It opens the file with the given file id.
/// If the file doesn't exist, it is creating it. The main difference
/// with `create` is that `open` doesn't generate a new UUID.
/// It returns the kv file with the file handler and the file size.
pub fn open(path: String, file_id: Int) -> Result(KVFile, file.Reason) {
  let file_name = get_file_name(path, file_id)
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
  let assert Ok(file_pid) = fs.open(file_name, [fs.Read, fs.Append])
  let assert Ok(file_info) = file.file_info(file_name)
  Ok(KVFile(file_id, file_pid, file_info.size, file_name))
}

/// Close a kv file. It closes the file handler.
pub fn close(kv_file: KVFile) -> Result(Nil, file.Reason) {
  let assert Ok(_) = fs.close(kv_file.handler)
  Ok(Nil)
}

/// Write a value to the kv file. It writes the value in the following
/// format:
/// - 4 bytes: the size of the value
/// - 1 byte: 0 if the value is not deleted, 1 if it is
/// - n bytes: the value
/// It returns the updated kv file with the new size.
pub fn write(kv_file: KVFile, value: Value) -> KVFile {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let value_size_bits = value_size_bits
  // end FIXME
  let value_size_bytes = value_size_bits / 8
  let assert Some(data) = value.data
  let data_size = bit_string.byte_size(data) + value_size_bytes + 1
  let deleted = case value.deleted {
    True -> 1
    False -> 0
  }
  let packed_data = <<
    data_size:size(value_size_bits),
    deleted:8,
    data:bit_string,
  >>
  let assert Ok(_) = fs.position(kv_file.handler, fs.Bof(value.offset))
  let assert Ok(_) = fs.write(kv_file.handler, packed_data)
  let data_size = bit_string.byte_size(packed_data)
  KVFile(..kv_file, size: kv_file.size + data_size)
}

/// Read a value from the kv file. It reads the value and returns it as
/// a BitString with the following format:
/// - 4 bytes: the size of the value
/// - 1 byte: 0 if the value is not deleted, 1 if it is
/// - n bytes: the value
pub fn read(kv_file: KVFile, offset: Int) -> Result(Value, file.Reason) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let value_size_bits = value_size_bits
  // end FIXME
  let value_size_bytes = value_size_bits / 8
  let assert Ok(_) = fs.position(kv_file.handler, fs.Bof(offset))
  let assert read.Ok(<<size:size(value_size_bits), deleted:8>>) =
    fs.read(kv_file.handler, value_size_bytes + 1)
  let content_size = size - value_size_bytes - 1
  let assert read.Ok(data) = fs.read(kv_file.handler, content_size)
  let deleted = case deleted {
    0 -> False
    1 -> True
  }
  Ok(Value(offset, deleted, Some(data), kv_file.id))
}
// pub fn vacuum(kv_file: KVFile) -> Result(KVFile, file.Reason) {
//   let assert Ok(_) = fs.position(kv_file.handler, fs.Bof(0))
//   let assert Ok(_) = fs.truncate(kv_file.handler, 0)
//   Ok(KVFile(..kv_file, size: 0))
// }
