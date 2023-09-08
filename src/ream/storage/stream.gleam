//// The event sourcing has the following actions:
//// 
//// - Append a new event to the corresponding stream.
//// - Retrieve a list of events based on the correlative ID.
//// 
//// That said, it's clear that if we don't need to modify that data we could
//// create files where that information could be placed with different limits,
//// we need to create a new file:
//// 
//// - based on the date, or
//// - based on the size of the file, or
//// - based on the number of elements inside the file.
//// 
//// The first one is not a good fit because if one stream isn't used only on
//// specific days it could mean there could be empty files and very populated
//// files. The second one looks good but it could be happening that each event
//// could be big enough to overflow the limit and we get a file per event. The
//// third one is a bit risky as well in the same case of the size, if the
//// events are big and the number is too high it could mean the file could be
//// huge and difficult to navigate.
//// 
//// I think the combination of the second of the third could give us a good
//// solution. I mean, we could define a minimum limit for the file and if that
//// limit is reached then one condition is achieved. The second condition
//// should be a minimum of elements in terms of getting that number of
//// elements even if the limit of the file was reached.
//// 
//// We could implement much more flexible ways for handling the files but for
//// the first version I think that the configuration for:
//// 
//// - `events.min_items_per_file` using a default value of 1000.
//// - `events.min_file_size` using a default value of 100M.
//// 
//// Then we are going to create files based on these parameters, and we are
//// going to be appending that information inside the files. The index for the
//// event sourcing is going to be another file where we are going to be
//// appending a 64-bit integer where the first 16 bits are going to be used to
//// identify the file and the remaining 48 bits are going to be used to store
//// the size where the event is starting. That said, we just defined some
//// limits:
//// 
//// - The maximum size for an event is 281,474,976,710,656 (2^48) or 256TB.
////   But I don't recommend using a size higher than 1MB for an event even
////   less if we are going to trigger thousands of events per second.
//// - The maximum number of files kept in the system will be 65,536 (2^16).
//// 
//// If we are sending events of 1KB based on the default configuration that
//// means we could store 100,000 events for each file or a total of
//// 6,553,600,000 events using 6TB approx.
//// 
//// The index for events is going to be useful when we need to retrieve an
//// event based on the correlative ID. Given the number we read that position
//// and retrieve the information about which file we should ask and the size
//// where that event is starting. The event storage will provide us with the
//// size of the event and the content for the event.

import gleam/bit_string
import gleam/erlang/file
import gleam/int
import gleam/iterator
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/stream/event.{EventFile}
import ream/storage/stream/index.{Index, IndexFile}

pub type Stream {
  Stream(
    name: String,
    index: IndexFile,
    active_file: Option(Int),
    files: Map(Int, EventFile),
    base_path: String,
    max_event_file_size: Int,
  )
}

pub fn get_base_path(path: String, name: String) -> String {
  fs.join([path, "stream", name])
}

pub fn open(
  name: String,
  path path: String,
  max_event_file_size max_event_file_size: Int,
) -> Result(Stream, file.Reason) {
  let base_path = fs.join([path, "stream", name])
  let assert Ok(index_file) = index.open(base_path)
  use files <- try(do_open_files(index_file, base_path, map.new()))
  let active_file = less_populated_file(map.values(files), max_event_file_size)
  Ok(Stream(
    name,
    index_file,
    active_file,
    files,
    base_path,
    max_event_file_size,
  ))
}

fn less_populated_file(
  files: List(EventFile),
  max_event_file_size: Int,
) -> Option(Int) {
  let files = list.sort(files, by: fn(a, b) { int.compare(a.size, b.size) })
  case list.first(files) {
    Ok(EventFile(id, _handler, size, _path)) if size < max_event_file_size ->
      Some(id)
    _ -> None
  }
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
  acc: Map(Int, EventFile),
) -> Result(Map(Int, EventFile), file.Reason) {
  case index.count(index_file) {
    0 -> Ok(acc)
    num_of_events -> {
      let files =
        num_of_events - 1
        |> iterator.range(0, _)
        |> iterator.fold(
          from: acc,
          with: fn(acc, idx) {
            let assert Ok(Index(_offset, _size, file_id)) =
              index.get(index_file, idx)
            case map.has_key(acc, file_id) {
              True -> acc
              False -> {
                let assert Ok(file) = event.open(path, file_id)
                map.insert(acc, file_id, file)
              }
            }
          },
        )

      Ok(files)
    }
  }
}

pub fn add_event(
  stream: Stream,
  event_content: BitString,
) -> Result(Stream, file.Reason) {
  let event_size = bit_string.byte_size(event_content)
  let #(stream, index) = case stream.active_file {
    None -> {
      let assert Ok(event_file) = event.create(stream.base_path)
      let event_file_id = event_file.id
      let #(index, index_file) =
        index.add(stream.index, event_size, event_file_id)
      let stream =
        Stream(
          ..stream,
          index: index_file,
          active_file: Some(event_file_id),
          files: map.insert(stream.files, event_file_id, event_file),
        )
      #(stream, index)
    }
    Some(active_file_id) -> {
      let #(index, index_file) =
        index.add(stream.index, event_size, active_file_id)
      let stream = Stream(..stream, index: index_file)
      #(stream, index)
    }
  }

  let assert Ok(event_file) = map.get(stream.files, index.file_id)
  let event_file = event.write(event_file, event_content)

  let files = map.insert(stream.files, event_file.id, event_file)
  case event_file.size >= stream.max_event_file_size {
    True -> Ok(Stream(..stream, active_file: None, files: files))
    False -> Ok(Stream(..stream, files: files))
  }
}

pub fn get_event(stream: Stream, index: Int) -> Result(BitString, file.Reason) {
  case index.count(stream.index) > index {
    True -> {
      let assert Ok(Index(offset, _size, file_id)) =
        index.get(stream.index, index)
      let assert Ok(file) = map.get(stream.files, file_id)
      event.read(file, offset)
    }
    False -> Error(file.Einval)
  }
}
