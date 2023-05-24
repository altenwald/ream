import gleam/bit_string
import gleam/erlang/file
import gleam/iterator
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/stream/file.{StreamFile} as sfile
import ream/storage/stream/index.{Index, IndexFile}

pub type Stream {
  Stream(
    name: String,
    index: IndexFile,
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
  index_file: IndexFile,
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
            let assert Ok(Index(_offset, _size, file_id)) =
              index.get_next(index_file)
            case map.has_key(acc, file_id) {
              True -> acc
              False -> map.insert(acc, file_id, sfile.open(path, file_id))
            }
          },
        )

      Ok(files)
    }
  }
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
      let assert Ok(stream_file) = sfile.create(stream.base_path)
      let #(index, index_file) =
        index.add(stream.index, event_size, stream_file.id)
      let stream =
        Stream(
          ..stream,
          index: index_file,
          active_file: Some(stream_file),
          files: map.insert(stream.files, stream_file.id, stream_file),
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
  let stream_file = sfile.write(file, event_content)

  let files = map.insert(stream.files, stream_file.id, stream_file)
  Ok(Stream(..stream, files: files))
}

pub fn get_event(stream: Stream, index: Int) -> Result(BitString, file.Reason) {
  case index.count(stream.index) > index {
    True -> {
      let assert Ok(Index(offset, size, file_id)) =
        index.get(stream.index, index)
      let assert Ok(file) = map.get(stream.files, file_id)
      sfile.read(file, offset, size)
    }
    False -> Error(file.Einval)
  }
}
