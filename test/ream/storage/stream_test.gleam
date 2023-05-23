import gleam/bit_string
import gleam/erlang/file
import gleam/map
import ream/storage/stream
import ream/storage/stream/index

pub fn open_test() {
  let _ = file.recursive_delete("build/stream_test/stream/entries")

  let assert Ok(entries) = stream.open("entries", path: "build/stream_test")
  let assert Ok(Nil) = stream.close(entries)
}

pub fn add_event_test() {
  let _ = file.recursive_delete("build/stream_test/stream/numbers")

  let assert Ok(numbers) = stream.open("numbers", path: "build/stream_test")
  let assert Ok(numbers) = stream.add_event(numbers, <<1, 2, 3, 4, 5>>)
  let assert 1 = map.size(numbers.files)
  let assert Ok(numbers) = stream.add_event(numbers, <<10, 20, 30, 40, 50>>)
  let assert 1 = map.size(numbers.files)
  let assert 2 = index.count(numbers.index)
  let assert Ok(Nil) = stream.close(numbers)
}

pub fn get_event_test() {
  let _ = file.recursive_delete("build/stream_test/stream/names")

  let assert Ok(names) = stream.open("names", path: "build/stream_test")
  let manuel_name = bit_string.from_string("Manuel")
  let marga_name = bit_string.from_string("Marga")
  let assert Ok(names) = stream.add_event(names, manuel_name)
  let assert Ok(names) = stream.add_event(names, marga_name)
  let assert 2 = index.count(names.index)
  let assert Ok(read_manuel_name) = stream.get_event(names, 0)
  let assert Ok(read_marga_name) = stream.get_event(names, 1)
  let assert Error(file.Einval) = stream.get_event(names, 2)
  let assert True = read_manuel_name == manuel_name
  let assert True = read_marga_name == marga_name
  let assert Ok(Nil) = stream.close(names)
}
