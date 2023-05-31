import gleam/list
import gleam/erlang/file
import gleam/result
import ream/storage/stream/event

const base_path = "build/stream_file_test/stream/"

pub fn open_and_close_ok_test() {
  let path = base_path <> "entries"
  let _ = file.recursive_delete(path)

  let <<file_id:128>> = <<0:128>>

  let assert Ok(entries) = event.open(path, file_id)
  let assert Ok(_) = event.close(entries)
}

pub fn create_and_close_ok_test() {
  let path = base_path <> "names"
  let _ = file.recursive_delete(path)

  let assert Ok(names) = event.create(path)
  let assert Ok(_) = event.close(names)
}

pub fn read_and_write_ok_test() {
  let path = base_path <> "zipcodes"
  let _ = file.recursive_delete(path)

  let assert Ok(zipcodes) = event.create(path)

  let zipcodes_list = [
    <<"1336AA":utf8>>,
    <<"1336BB":utf8>>,
    <<"1336CC":utf8>>,
    <<"14100":utf8>>,
    <<"WC8 1DD":utf8>>,
  ]

  let zipcodes_offsets = [0, 9, 18, 27, 35]

  let zipcodes =
    list.fold(
      zipcodes_list,
      zipcodes,
      fn(acc, zipcode) { event.write(acc, zipcode) },
    )

  let assert True =
    zipcodes_list == list.map(
      zipcodes_offsets,
      fn(i) { result.unwrap(event.read(zipcodes, i), <<>>) },
    )

  let assert Ok(_) = event.close(zipcodes)
}
