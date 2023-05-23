import gleam/bit_string
import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/int
import gleam/iterator
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import gleam/result.{try}
import gleam/string
import ids/uuid
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/stream/index

pub type StreamFile {
  StreamFile(id: Int, handler: Pid, size: Int, file_path: String)
}

pub type Stream {
  Stream(
    name: String,
    index: index.IndexFile,
    active_file: Option(StreamFile),
    files: Map(Int, StreamFile),
    base_path: String,
  )
}

pub fn get_base_path(path: String, name: String) -> String {
  fs.join([path, "stream", name])
}

pub fn open(name: String, path path: String) -> Result(Stream, file.Reason) {
  let base_path = fs.join([path, "stream", name])
  let assert Ok(index_file) = index.open(base_path)
  use files <- try(do_open_files(index_file, base_path, map.new()))
  let active_file = less_populated_file(map.values(files))
  Ok(Stream(name, index_file, active_file, files, base_path))
}

fn less_populated_file(files: List(StreamFile)) -> Option(StreamFile) {
  files
  |> list.fold(
    with: fn(acc, file) {
      let file_size = file.size
      case acc {
        Some(StreamFile(_id, _handler, size, _file_id)) if file_size >= size ->
          acc
        _ -> Some(file)
      }
    },
    from: None,
  )
}

pub fn close(stream: Stream) -> Result(Nil, file.Reason) {
  let assert Ok(_) = index.close(stream.index)

  stream.files
  |> map.values()
  |> list.each(fn(file) { fs.close(file.handler) })

  Ok(Nil)
}

fn do_open_files(
  index_file: index.IndexFile,
  path: String,
  acc: Map(Int, StreamFile),
) -> Result(Map(Int, StreamFile), file.Reason) {
  case index.count(index_file) {
    0 -> Ok(acc)
    num_of_events -> {
      let _ = index.set_pos(index_file, 0)

      let files =
        num_of_events - 1
        |> iterator.range(0, _)
        |> iterator.fold(
          from: acc,
          with: fn(acc, _idx) {
            let assert Ok(index.Index(_offset, _size, file_id)) =
              index.get_next(index_file)
            case map.has_key(acc, file_id) {
              True -> acc
              False -> {
                let file_name = get_file_name(path, file_id)
                let assert Ok(file_info) = file.file_info(file_name)
                let assert Ok(file_pid) =
                  fs.open(file_name, [fs.Read, fs.Append])
                map.insert(
                  acc,
                  file_id,
                  StreamFile(file_id, file_pid, file_info.size, file_name),
                )
              }
            }
          },
        )

      Ok(files)
    }
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
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(file_name))
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
  let #(stream, index) = case index.count(stream.index) {
    0 -> {
      let assert Ok(stream_file) = add_file(stream)
      let file_id = stream_file.id
      let #(index, index_file) =
        index.add(stream.index, event_size, stream_file.id)
      let stream =
        Stream(
          ..stream,
          index: index_file,
          active_file: Some(stream_file),
          files: map.insert(stream.files, file_id, stream_file),
        )
      #(stream, index)
    }
    _ -> {
      let assert Some(active_file) = stream.active_file
      let #(index, index_file) =
        index.add(stream.index, event_size, active_file.id)
      let stream = Stream(..stream, index: index_file)
      #(stream, index)
    }
  }

  let assert Ok(file) = map.get(stream.files, index.file_id)
  let assert Ok(_) = fs.write(file.handler, event_content)

  let stream_file = StreamFile(..file, size: file.size + event_size)
  let files = map.insert(stream.files, stream_file.id, stream_file)

  Ok(Stream(..stream, files: files))
}

pub fn get_event(stream: Stream, index: Int) -> Result(BitString, file.Reason) {
  case index.count(stream.index) > index {
    True -> {
      let assert Ok(index.Index(offset, size, file_id)) =
        index.get(stream.index, index)
      let assert Ok(file) = map.get(stream.files, file_id)
      let assert Ok(_) = fs.position(file.handler, fs.Bof(offset))
      let assert read.Ok(<<_size:32, event:binary>>) =
        fs.read(file.handler, size)
      Ok(event)
    }
    False -> Error(file.Einval)
  }
}
