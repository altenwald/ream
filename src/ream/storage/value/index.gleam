import gleam/erlang/process.{Subject}
import gleam/erlang/file
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/value.{ValueFile}

pub type ValueIndex {
  ValueIndex(
    base_path: String,
    max_value_size: Int,
    active_value_file: Option(Int),
    values: Map(Int, ValueFile),
  )
}

pub fn size(value_index: ValueIndex) -> Int {
  map.size(value_index.values)
}

pub fn byte_size(value_index: ValueIndex) -> Int {
  map.fold(
    value_index.values,
    0,
    fn(acc, _key, value_file) { acc + value_file.size },
  )
}

pub fn load(path: String, max_value_size: Int) -> ValueIndex {
  let value_index_file = fs.join([path, "index"])
  let assert Ok(True) =
    fs.recursive_make_directory(fs.dirname(value_index_file))
  let assert Ok(value_index) = fs.open(value_index_file, [fs.Read, fs.Write])
  let #(active_value_file, values) =
    read_value_files(value_index, path, None, max_value_size, map.new())
  let assert Ok(_) = fs.close(value_index)

  ValueIndex(
    base_path: path,
    max_value_size: max_value_size,
    active_value_file: active_value_file,
    values: values,
  )
}

pub fn unload(value_index: ValueIndex) -> Result(Nil, Nil) {
  // TODO: suggest map.each/2
  map.filter(
    value_index.values,
    fn(_id, v) {
      let assert Ok(_) = value.close(v)
      False
    },
  )

  Ok(Nil)
}

fn read_value_files(
  value_index: Subject(fs.Message),
  path: String,
  last_file_id: Option(Int),
  max_value_size: Int,
  acc: Map(Int, ValueFile),
) -> #(Option(Int), Map(Int, ValueFile)) {
  case fs.read(value_index, fs.Cur(0), 16) {
    read.Ok(<<file_id:size(128)>>) -> {
      let assert Ok(value_file) = value.open(path, file_id, max_value_size)
      read_value_files(
        value_index,
        path,
        Some(file_id),
        max_value_size,
        map.insert(acc, file_id, value_file),
      )
    }
    read.Eof -> #(last_file_id, acc)
    read.Error(_err) -> {
      let assert Ok(_) = fs.close(value_index)
      panic as "cannot read from file"
    }
  }
}

pub fn flush(value_index: ValueIndex) -> Result(Bool, file.Reason) {
  let value_index_file = fs.join([value_index.base_path, "index"])
  let assert Ok(True) = fs.recursive_make_directory(value_index.base_path)
  let assert Ok(vi_file) = fs.open(value_index_file, [fs.Write])
  let assert Ok(_) = write_values(vi_file, map.keys(value_index.values))
  fs.close(vi_file)
}

fn write_values(
  index_value: Subject(fs.Message),
  values: List(Int),
) -> Result(Nil, Nil) {
  case values {
    [id, ..rest] -> {
      let assert Ok(_) = fs.write(index_value, fs.Cur(0), <<id:128>>)
      write_values(index_value, rest)
    }
    [] -> Ok(Nil)
  }
}

pub fn get(value_index: ValueIndex, vfile_id: Int) -> Result(ValueFile, Nil) {
  map.get(value_index.values, vfile_id)
}

pub fn get_active(value_index: ValueIndex) -> Result(ValueFile, Nil) {
  let assert Some(vfile_id) = value_index.active_value_file
  get(value_index, vfile_id)
}

pub fn update_active(value_index: ValueIndex) -> ValueIndex {
  case value_index.active_value_file {
    Some(_file_id) -> value_index
    None -> {
      let assert Ok(vfile) =
        value.create(value_index.base_path, value_index.max_value_size)
      set(value_index, vfile)
    }
  }
}

pub fn set(value_index: ValueIndex, vfile: ValueFile) -> ValueIndex {
  ValueIndex(
    ..value_index,
    active_value_file: Some(vfile.id),
    values: map.insert(value_index.values, vfile.id, vfile),
  )
}
