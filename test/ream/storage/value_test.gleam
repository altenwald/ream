import gleam/list
import gleam/erlang/file
import gleam/option.{None, Some}
import gleam/result
import ream/storage/value.{Value, ValueFileInfo}

const base_path = "build/value_test/kv/"

const max_file_size = 1024

pub fn open_and_close_test() {
  let path = base_path <> "entries"
  let _ = file.recursive_delete(path)

  let <<file_id:128>> = <<0:128>>

  let assert Ok(entries) = value.open(path, file_id, max_file_size)
  let assert Ok(_) = value.close(entries)
}

pub fn create_and_close_test() {
  let path = base_path <> "names"
  let _ = file.recursive_delete(path)

  let assert Ok(names) = value.create(path, max_file_size)
  let assert Ok(_) = value.close(names)
}

pub fn read_and_write_test() {
  let path = base_path <> "zipcodes"
  let _ = file.recursive_delete(path)

  let assert Ok(zipcodes) = value.create(path, max_file_size)

  let file_id = zipcodes.id

  let zipcodes_list = [
    Value(0, False, Some(<<"1336AA":utf8>>), file_id),
    Value(11, False, Some(<<"1336BB":utf8>>), file_id),
    Value(22, False, Some(<<"1336CC":utf8>>), file_id),
    Value(33, False, Some(<<"14100":utf8>>), file_id),
    Value(43, False, Some(<<"WC8 1DD":utf8>>), file_id),
  ]

  let zipcodes_offsets = list.map(zipcodes_list, fn(v) { v.offset })

  let zipcodes =
    list.fold(
      zipcodes_list,
      zipcodes,
      fn(acc, zipcode) {
        let assert Ok(value) = value.write_value(acc, zipcode)
        value
      },
    )

  let assert True =
    zipcodes_list == list.map(
      zipcodes_offsets,
      fn(i) {
        result.unwrap(value.read(zipcodes, i), Value(0, False, None, file_id))
      },
    )

  let assert Ok(_) = value.close(zipcodes)
}

pub fn delete_test() {
  let path = base_path <> "animals"
  let _ = file.recursive_delete(path)

  let assert Ok(animals) = value.create(path, max_file_size)

  let file_id = animals.id

  let bulls = Value(21, False, Some(<<"bulls":utf8>>), file_id)
  let animals_list = [
    Value(0, False, Some(<<"mouses":utf8>>), file_id),
    Value(11, False, Some(<<"bears":utf8>>), file_id),
    bulls,
    Value(31, False, Some(<<"dogs":utf8>>), file_id),
    Value(40, False, Some(<<"elephants":utf8>>), file_id),
  ]

  let animals_offsets = list.map(animals_list, fn(v) { v.offset })

  let animals =
    list.fold(
      animals_list,
      animals,
      fn(acc, animal) {
        let assert Ok(value) = value.write_value(acc, animal)
        value
      },
    )

  let assert True =
    animals_list == list.map(
      animals_offsets,
      fn(i) {
        result.unwrap(value.read(animals, i), Value(0, False, None, file_id))
      },
    )

  let assert Ok(animals) = value.delete(animals, bulls)

  let animals_list = [
    Value(0, False, Some(<<"mouses":utf8>>), file_id),
    Value(11, False, Some(<<"bears":utf8>>), file_id),
    Value(21, True, Some(<<"bulls":utf8>>), file_id),
    Value(31, False, Some(<<"dogs":utf8>>), file_id),
    Value(40, False, Some(<<"elephants":utf8>>), file_id),
  ]

  let assert True =
    animals_list == list.map(
      animals_offsets,
      fn(i) {
        result.unwrap(value.read(animals, i), Value(0, False, None, file_id))
      },
    )

  let assert ValueFileInfo(read_file_id, 54, 1024, 5, 1) =
    value.get_file_info(animals)
  let assert True = read_file_id == file_id
  let assert Ok(_) = value.close(animals)
}
