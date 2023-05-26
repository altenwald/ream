import gleam/list
import gleam/erlang/file
import gleam/option.{None, Some}
import gleam/result
import ream/storage/kv/file as kv_file

const base_path = "build/kv_file_test/stream/"

pub fn open_and_close_ok_test() {
  let path = base_path <> "entries"
  let _ = file.recursive_delete(path)

  let <<file_id:128>> = <<0:128>>

  let assert Ok(entries) = kv_file.open(path, file_id)
  let assert Ok(_) = kv_file.close(entries)
}

pub fn create_and_close_ok_test() {
  let path = base_path <> "names"
  let _ = file.recursive_delete(path)

  let assert Ok(names) = kv_file.create(path)
  let assert Ok(_) = kv_file.close(names)
}

pub fn read_and_write_ok_test() {
  let path = base_path <> "zipcodes"
  let _ = file.recursive_delete(path)

  let assert Ok(zipcodes) = kv_file.create(path)

  let file_id = zipcodes.id

  let zipcodes_list = [
    kv_file.Value(0, False, Some(<<"1336AA":utf8>>), file_id),
    kv_file.Value(11, False, Some(<<"1336BB":utf8>>), file_id),
    kv_file.Value(22, False, Some(<<"1336CC":utf8>>), file_id),
    kv_file.Value(33, False, Some(<<"14100":utf8>>), file_id),
    kv_file.Value(43, False, Some(<<"WC8 1DD":utf8>>), file_id),
  ]

  let zipcodes_offsets = [0, 11, 22, 33, 43]

  let zipcodes =
    list.fold(
      zipcodes_list,
      zipcodes,
      fn(acc, zipcode) { kv_file.write(acc, zipcode) },
    )

  let assert True =
    zipcodes_list == list.map(
      zipcodes_offsets,
      fn(i) {
        result.unwrap(
          kv_file.read(zipcodes, i),
          kv_file.Value(0, False, None, file_id),
        )
      },
    )

  let assert Ok(_) = kv_file.close(zipcodes)
}
