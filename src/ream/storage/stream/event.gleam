//// Store the information of the event that is being streamed. It is used to
//// write and read events from the file.
////
//// The events stored in the file are in the following format:
//// - 4 bytes: the size of the event
//// - n bytes: the event

import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/bit_string
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/stream/index
import ream/uuid

/// The information for the event file. The fields are:
/// - `id`: the id of the file. It's intended to be a UUID but it's stored as an Int.
/// - `handler`: the file handler to read and write events.
/// - `size`: the size of the file.
/// - `file_path`: the path of the file.
pub type EventFile {
  EventFile(id: Int, handler: Pid, size: Int, file_path: String)
}

/// Create a new event file. It creates a new file with a random UUID as the
/// path for finding the file.
///
/// For example, if the UUID is `f81d4fae-7dec-11d0-a765-00a0c91e6bf6`, the
/// file will be created in the following path:
/// `base_path/f81d4fae/7dec/11d0/a765/00a0c91e6bf6`.
pub fn create(base_path: String) -> Result(EventFile, file.Reason) {
  let <<file_id:128>> = uuid.new()
  let file_name = get_file_name(base_path, file_id)
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
  use file_pid <- try(fs.open(file_name, [fs.Read, fs.Append]))
  Ok(EventFile(file_id, file_pid, 0, file_name))
}

fn get_file_name(base_path: String, file_id: Int) -> String {
  fs.join([base_path, ..uuid.parts(<<file_id:128>>)])
}

/// Open a event file. It opens the file with the given file id.
/// If the file doesn't exist, it is creating it. The main difference
/// with `create` is that `open` doesn't generate a new UUID.
/// It returns the event file with the file handler and the file size.
pub fn open(path: String, file_id: Int) -> Result(EventFile, file.Reason) {
  let file_name = get_file_name(path, file_id)
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
  let assert Ok(file_pid) = fs.open(file_name, [fs.Read, fs.Append])
  let assert Ok(file_info) = file.file_info(file_name)
  Ok(EventFile(file_id, file_pid, file_info.size, file_name))
}

/// Close an event file. It closes the file handler.
pub fn close(event_file: EventFile) -> Result(Nil, file.Reason) {
  let assert Ok(_) = fs.close(event_file.handler)
  Ok(Nil)
}

/// Write an event to the event file. It writes the event in the following
/// format:
/// - 4 bytes: the size of the event
/// - n bytes: the event
/// It returns the updated event file with the new size.
pub fn write(event_file: EventFile, event_data: BitString) -> EventFile {
  let event_size_bits = index.event_size_bits
  let event_size_bytes = event_size_bits / 8
  let data_size = bit_string.byte_size(event_data) + event_size_bytes
  let data = <<data_size:size(event_size_bits), event_data:bit_string>>
  let assert Ok(_) = fs.position(event_file.handler, fs.Eof(0))
  let assert Ok(_) = fs.write(event_file.handler, data)
  let data_size = bit_string.byte_size(data)
  EventFile(..event_file, size: event_file.size + data_size)
}

/// Read an event from the event file. It reads the event and returns it as
/// a BitString with the following format:
/// - 4 bytes: the size of the event
/// - n bytes: the event
pub fn read(
  event_file: EventFile,
  offset: Int,
) -> Result(BitString, file.Reason) {
  let event_size_bits = index.event_size_bits
  let event_size_bytes = event_size_bits / 8
  let assert Ok(_) = fs.position(event_file.handler, fs.Bof(offset))
  let assert read.Ok(<<size:size(event_size_bits)>>) =
    fs.read(event_file.handler, event_size_bytes)
  let content_size = size - event_size_bytes
  let assert read.Ok(data) = fs.read(event_file.handler, content_size)
  Ok(data)
}
