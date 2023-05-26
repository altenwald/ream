import gleam/bit_string
import gleam/list
import gleam/erlang/file
import gleam/result
import ream/storage/stream/file as sfile

const base_path = "build/stream_file_test/stream/"

pub fn open_and_close_ok_test() {
  let path = base_path <> "entries"
  let _ = file.recursive_delete(path)

  let <<file_id:128>> = <<0:128>>

  let assert Ok(entries) = sfile.open(path, file_id)
  let assert Ok(_) = sfile.close(entries)
}

pub fn create_and_close_ok_test() {
  let path = base_path <> "names"
  let _ = file.recursive_delete(path)

  let assert Ok(names) = sfile.create(path)
  let assert Ok(_) = sfile.close(names)
}

pub fn read_and_write_ok_test() {
  let path = base_path <> "zipcodes"
  let _ = file.recursive_delete(path)

  let assert Ok(zipcodes) = sfile.create(path)

  let zipcodes_list = [
    sfile.Event(0, bit_string.from_string("1336AA")),
    sfile.Event(9, bit_string.from_string("1336BB")),
    sfile.Event(18, bit_string.from_string("1336CC")),
    sfile.Event(27, bit_string.from_string("14100")),
    sfile.Event(35, bit_string.from_string("WC8 1DD")),
  ]

  let zipcodes_offsets = [0, 9, 18, 27, 35]

  let zipcodes =
    list.fold(
      zipcodes_list,
      zipcodes,
      fn(acc, zipcode) { sfile.write(acc, zipcode) },
    )

  let assert True =
    zipcodes_list == list.map(
      zipcodes_offsets,
      fn(i) { result.unwrap(sfile.read(zipcodes, i), sfile.Event(0, <<>>)) },
    )

  let assert Ok(_) = sfile.close(zipcodes)
}
