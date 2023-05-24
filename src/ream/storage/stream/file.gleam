//// Store the information of the file that is being streamed. It is used to
//// write and read events from the file.
////
//// The events stored in the file are in the following format:
//// - 4 bytes: the size of the event
//// - n bytes: the event

import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/bit_string
import gleam/int
import gleam/result.{try}
import gleam/string
import ids/uuid
import ream/storage/file as fs
import ream/storage/file/read

/// The information for the stream file. The fields are:
/// - `id`: the id of the file. It's intended to be a UUID but it's stored as an Int.
/// - `handler`: the file handler to read and write events.
/// - `size`: the size of the file.
/// - `file_path`: the path of the file.
pub type StreamFile {
  StreamFile(id: Int, handler: Pid, size: Int, file_path: String)
}

/// Create a new stream file. It creates a new file with a random UUID as the
/// path for finding the file.
///
/// For example, if the UUID is `f81d4fae-7dec-11d0-a765-00a0c91e6bf6`, the
/// file will be created in the following path:
/// `base_path/f81d4fae/7dec/11d0/a765/00a0c91e6bf6`.
pub fn create(base_path: String) -> Result(StreamFile, file.Reason) {
  let assert Ok(uuid) = uuid.generate_v4()
  let file_id = uuid_to_int(uuid)
  let file_name = get_file_name(base_path, file_id)
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
  use file_pid <- try(fs.open(file_name, [fs.Read, fs.Append]))
  Ok(StreamFile(file_id, file_pid, 0, file_name))
}

fn uuid_to_int(uuid: String) -> Int {
  let base_uuid = string.replace(uuid, "-", "")
  let assert Ok(uuid_int) = int.base_parse(base_uuid, 16)
  uuid_int
}

fn get_file_name(base_path: String, file_id: Int) -> String {
  let <<p1:32, p2:16, p3:16, p4:16, p5:48>> = <<file_id:128>>
  let p1 = string.pad_left(int.to_base16(p1), to: 8, with: "0")
  let p2 = string.pad_left(int.to_base16(p2), to: 4, with: "0")
  let p3 = string.pad_left(int.to_base16(p3), to: 4, with: "0")
  let p4 = string.pad_left(int.to_base16(p4), to: 4, with: "0")
  let p5 = string.pad_left(int.to_base16(p5), to: 12, with: "0")
  fs.join([base_path, p1, p2, p3, p4, p5])
}

/// Open a stream file. It opens the file with the given id.
pub fn open(path: String, file_id: Int) -> StreamFile {
  let file_name = get_file_name(path, file_id)
  let assert Ok(file_info) = file.file_info(file_name)
  let assert Ok(file_pid) = fs.open(file_name, [fs.Read, fs.Append])
  StreamFile(file_id, file_pid, file_info.size, file_name)
}

/// Close a stream file. It closes the file handler.
pub fn close(stream_file: StreamFile) -> Result(StreamFile, file.Reason) {
  let assert Ok(_) = fs.close(stream_file.handler)
  Ok(stream_file)
}

/// Write an event to the stream file. It writes the event in the following
/// format:
/// - 4 bytes: the size of the event
/// - n bytes: the event
/// It returns the updated stream file with the new size.
pub fn write(stream_file: StreamFile, data: BitString) -> StreamFile {
  let assert Ok(_) = fs.write(stream_file.handler, data)
  let data_size = bit_string.byte_size(data)
  StreamFile(..stream_file, size: stream_file.size + data_size)
}

/// Read an event from the stream file. It reads the event and returns it as
/// a BitString with the following format:
/// - 4 bytes: the size of the event
/// - n bytes: the event
pub fn read(
  stream_file: StreamFile,
  offset: Int,
  size: Int,
) -> Result(BitString, file.Reason) {
  let assert Ok(_) = fs.position(stream_file.handler, fs.Bof(offset))
  let assert read.Ok(<<_size:32, event:binary>>) =
    fs.read(stream_file.handler, size)
  Ok(event)
}
