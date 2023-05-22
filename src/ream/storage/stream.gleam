import gleam/bit_string
import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/int
import gleam/list
import gleam/map.{Map}
import gleam/result.{try}
import gleam/string
import ids/uuid
import ream/storage/file as fs
import ream/storage/file/read

pub const event_max_size: Int = 32

pub type Index {
  Index(offset: Int, size: Int, file_id: Int)
}

pub type StreamFile {
  StreamFile(id: Int, handler: Pid, size: Int, file_path: String)
}

pub type IndexFile {
  IndexFile(handler: Pid, size: Int, file_path: String)
}

pub type Stream {
  Stream(
    name: String,
    index: IndexFile,
    files: Map(Int, StreamFile),
    base_path: String,
  )
}

pub fn open(name: String, path path: String) -> Result(Stream, file.Reason) {
  let base_path = fs.join([path, "stream", name])
  let assert Ok(True) = recursive_make_directory(base_path)
  let index = fs.join([base_path, "index"])
  use index_pid <- try(fs.open(index, [fs.Read, fs.Append]))
  use index_info <- try(file.file_info(index))
  use files <- try(do_open_files(index_pid, base_path, map.new()))
  Ok(Stream(
    name,
    IndexFile(index_pid, index_info.size, index),
    files,
    base_path,
  ))
}

fn recursive_make_directory(path: String) -> Result(Bool, file.Reason) {
  case file.is_directory(path) {
    Error(file.Enoent) -> {
      let prev_dir = fs.dirname(path)
      let assert Ok(True) = recursive_make_directory(prev_dir)
      let assert Ok(_) = file.make_directory(path)
      Ok(True)
    }
    Error(file.Eexist) -> Ok(True)
    Ok(True) -> Ok(True)
    _ -> Error(file.Einval)
  }
}

pub fn close(stream: Stream) -> Result(Nil, file.Reason) {
  let assert Ok(_) = fs.close(stream.index.handler)

  stream.files
  |> map.values()
  |> list.each(fn(file) { fs.close(file.handler) })

  Ok(Nil)
}

fn do_open_files(
  index_pid: Pid,
  path: String,
  acc: Map(Int, StreamFile),
) -> Result(Map(Int, StreamFile), file.Reason) {
  case fs.read(index_pid, 26) {
    read.Ok(<<_offset:48, _size:32, file_id:128>>) -> {
      case map.has_key(acc, file_id) {
        True -> do_open_files(index_pid, path, acc)
        False -> {
          let file_name = get_file_name(path, file_id)
          use file_info <- try(file.file_info(file_name))
          use file_pid <- try(fs.open(file_name, [fs.Read, fs.Append]))
          do_open_files(
            index_pid,
            path,
            map.insert(
              acc,
              file_id,
              StreamFile(file_id, file_pid, file_info.size, file_name),
            ),
          )
        }
      }
    }

    read.Eof -> Ok(acc)
    read.Error(reason) -> Error(reason)
  }
}

fn get_file_name(path: String, file_id: Int) -> String {
  let <<p1:64, p2:32, p3:32, p4:32, p5:96>> =
    file_id
    |> int.to_base16()
    |> string.pad_left(to: 32, with: "0")
    |> bit_string.from_string()

  let p1 = string.pad_left(int.to_base16(p1), to: 8, with: "0")
  let p2 = string.pad_left(int.to_base16(p2), to: 4, with: "0")
  let p3 = string.pad_left(int.to_base16(p3), to: 4, with: "0")
  let p4 = string.pad_left(int.to_base16(p4), to: 4, with: "0")
  let p5 = string.pad_left(int.to_base16(p5), to: 12, with: "0")
  fs.join([path, p1, p2, p3, p4, p5])
}

fn add_file(stream: Stream) -> Result(StreamFile, file.Reason) {
  let assert Ok(uuid) = uuid.generate_v4()
  let file_id = uuid_to_int(uuid)
  let file_name = get_file_name(stream.base_path, file_id)
  let assert Ok(True) = recursive_make_directory(fs.dirname(file_name))
  use file_pid <- try(fs.open(file_name, [fs.Read, fs.Append]))
  Ok(StreamFile(file_id, file_pid, 0, file_name))
}

fn uuid_to_int(uuid: String) -> Int {
  let <<p1:64, _:8, p2:32, _:8, p3:32, _:8, p4:32, _:8, p5:96>> =
    bit_string.from_string(uuid)

  let assert Ok(uuid) =
    bit_string.to_string(<<p1:64, p2:32, p3:32, p4:32, p5:96>>)
  let assert Ok(uuid_int) = int.base_parse(uuid, 16)
  uuid_int
}

pub fn add_event(
  stream: Stream,
  event: BitString,
) -> Result(Stream, file.Reason) {
  // add 4 (32 / 8)
  let event_size = bit_string.byte_size(event) + 4
  let event_content = bit_string.concat([<<event_size:32>>, event])
  let #(stream, index) = case stream.index.size {
    0 -> {
      let assert Ok(stream_file) = add_file(stream)
      let file_id = stream_file.id
      let stream =
        Stream(..stream, files: map.insert(stream.files, file_id, stream_file))
      #(stream, Index(0, event_size, file_id))
    }
    _ -> {
      let assert Ok(_) = fs.position(stream.index.handler, fs.Eof(-26))
      let assert read.Ok(<<offset:48, prev_size:32, file_id:128>>) =
        fs.read(stream.index.handler, 26)
      #(stream, Index(offset + prev_size, event_size, file_id))
    }
  }

  let index_content = <<index.offset:48, index.size:32, index.file_id:128>>
  let assert Ok(_) = fs.write(stream.index.handler, index_content)
  let assert Ok(file) = map.get(stream.files, index.file_id)
  let assert Ok(_) = fs.write(file.handler, event_content)

  let index_file = IndexFile(..stream.index, size: stream.index.size + 26)
  let stream_file = StreamFile(..file, size: file.size + event_size)
  let files = map.insert(stream.files, stream_file.id, stream_file)

  Ok(Stream(..stream, index: index_file, files: files))
}

pub fn get_num_of_events(stream: Stream) -> Int {
  let assert Ok(result) = int.divide(stream.index.size, 26)
  result
}

pub fn get_event(stream: Stream, index: Int) -> Result(BitString, file.Reason) {
  case get_num_of_events(stream) > index {
    True -> {
      let assert Ok(_) = fs.position(stream.index.handler, fs.Bof(index * 26))
      let assert read.Ok(<<offset:48, size:32, file_id:128>>) =
        fs.read(stream.index.handler, 26)
      let assert Ok(file) = map.get(stream.files, file_id)
      let assert Ok(_) = fs.position(file.handler, fs.Bof(offset))
      let assert read.Ok(<<_size:32, event:binary>>) =
        fs.read(file.handler, size)
      Ok(event)
    }
    False -> Error(file.Einval)
  }
}
