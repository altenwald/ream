//// Store the information of the values for kv.
////
//// The events stored in the file are in the following format:
//// - 4 bytes: the size of the event
//// - 1 byte: 0 if the event is not deleted, 1 if it is
//// - n bytes: the event
////
//// The KV system is split into two parts: the key store and the value store.
//// The file is related to the value store. As we said it's a file with the
//// content marking the size of the value, if it's deleted and the value itself.
////
//// The file is append only. When a value is written, it's appended to the end
//// of the file. When a value is deleted, it's not deleted from the file, it's
//// marked as deleted. When a value is updated, it's deleted and a new value is
//// appended to the end of the file.

import gleam/bit_string
import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/option.{Option, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/file/read
import ream/uuid

pub const value_size_bits = 32

/// The information for a value. The fields are:
/// - `offset`: the offset of the value in the file.
/// - `deleted`: if the value is deleted.
/// - `data`: the value. It can be `None` if we didn't read the value yet.
/// - `file_id`: the id of the file where the value is stored.
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

pub type Info {
  Info(file_id: Int, size: Int, entries: Int, deleted: Int)
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
  use file_pid <- try(fs.open(file_name, [fs.Read, fs.Write]))
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
  let assert Ok(file_pid) = fs.open(file_name, [fs.Read, fs.Write])
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
  let assert Ok(offset) = fs.position(kv_file.handler, fs.Bof(value.offset))
  let assert Ok(_) = fs.write(kv_file.handler, packed_data)
  case offset + data_size > kv_file.size {
    True -> KVFile(..kv_file, size: offset + data_size)
    False -> kv_file
  }
}

/// Delete a value from the kv file. It marks the value as deleted.
/// It returns the updated kv file with the new size.
pub fn delete(kv_file: KVFile, value: Value) -> Result(KVFile, file.Reason) {
  case read(kv_file, value.offset) {
    Ok(value) -> Ok(write(kv_file, Value(..value, deleted: True)))
    Error(reason) -> Error(reason)
  }
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

pub fn get_file_info(kv_file: KVFile) -> Info {
  let assert Ok(_) = fs.position(kv_file.handler, fs.Bof(0))
  let assert Ok(#(entries, deleted)) =
    get_entries_and_deleted(kv_file.handler, #(0, 0))
  Info(kv_file.id, kv_file.size, entries, deleted)
}

fn get_entries_and_deleted(
  handler: Pid,
  acc: #(Int, Int),
) -> Result(#(Int, Int), file.Reason) {
  // FIXME: https://github.com/gleam-lang/gleam/issues/2166
  let value_size_bits = value_size_bits
  // end FIXME
  let value_size_bytes = value_size_bits / 8
  case fs.read(handler, value_size_bytes + 1) {
    read.Ok(<<size:size(value_size_bits), deleted:8>>) -> {
      let payload_size = size - value_size_bytes - 1
      let assert Ok(_) = fs.position(handler, fs.Cur(payload_size))
      get_entries_and_deleted(handler, #(acc.0 + 1, acc.1 + deleted))
    }
    read.Eof -> Ok(acc)
    read.Error(reason) -> Error(reason)
  }
}
