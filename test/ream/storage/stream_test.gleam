import gleam/bit_string
import gleam/erlang/file
import gleam/map
import ream/storage/stream
import ream/storage/stream/index

pub fn open_test() {
  let _ = file.recursive_delete("build/stream_test/stream/entries")

  let assert Ok(entries) =
    stream.open(
      "entries",
      path: "build/stream_test",
      max_event_file_size: 1_048_576,
    )
  let assert Ok(Nil) = stream.close(entries)
}

pub fn add_event_test() {
  let _ = file.recursive_delete("build/stream_test/stream/numbers")

  let event1 = <<1, 2, 3, 4, 5>>
  let event2 = <<10, 20, 30, 40, 50>>
  let assert Ok(numbers) =
    stream.open(
      "numbers",
      path: "build/stream_test",
      max_event_file_size: 1_048_576,
    )
  let assert Ok(numbers) = stream.add_event(numbers, event1)
  let assert 1 = map.size(numbers.files)
  let assert Ok(numbers) = stream.add_event(numbers, event2)
  let assert 1 = map.size(numbers.files)
  let assert 2 = index.count(numbers.index)
  let assert True = Ok(event1) == stream.get_event(numbers, 0)
  let assert True = Ok(event2) == stream.get_event(numbers, 1)
  let assert Error(file.Einval) = stream.get_event(numbers, 2)
  let assert Ok(Nil) = stream.close(numbers)
}

pub fn add_event_overflow_test() {
  let _ = file.recursive_delete("build/stream_test/stream/letters")

  let event1 = <<
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ":utf8, "abcdefghijklmnopqrstuvwxyz":utf8,
  >>
  let event2 = <<
    "01234567890123456789012345":utf8, "=!@#$%&/()=!@#$%&/()=!@#$%":utf8,
  >>
  let assert Ok(letters) =
    stream.open(
      "letters",
      path: "build/stream_test",
      max_event_file_size: bit_string.byte_size(event1) - 1,
    )

  let assert Ok(letters) = stream.add_event(letters, event1)
  let assert 1 = map.size(letters.files)
  let assert Ok(letters) = stream.add_event(letters, event2)
  let assert 2 = map.size(letters.files)
  let assert 2 = index.count(letters.index)
  let assert True = Ok(event1) == stream.get_event(letters, 0)
  let assert True = Ok(event2) == stream.get_event(letters, 1)
  let assert Error(file.Einval) = stream.get_event(letters, 2)
  let assert Ok(Nil) = stream.close(letters)
}

pub fn get_event_test() {
  let _ = file.recursive_delete("build/stream_test/stream/names")

  let assert Ok(names) =
    stream.open(
      "names",
      path: "build/stream_test",
      max_event_file_size: 1_048_576,
    )
  let manuel_name = <<"Manuel":utf8>>
  let marga_name = <<"Marga":utf8>>
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
