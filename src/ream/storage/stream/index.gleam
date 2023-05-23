import gleam/erlang/file
import gleam/erlang/process.{Pid}
import gleam/int
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/file/read

pub type Index {
  Index(offset: Int, size: Int, file_id: Int)
}

pub type IndexFile {
  IndexFile(handler: Pid, size: Int, file_path: String)
}

pub fn open(path: String) -> Result(IndexFile, file.Reason) {
  let assert Ok(True) = fs.recursive_make_directory(path)
  let index = fs.join([path, "index"])
  use index_pid <- try(fs.open(index, [fs.Read, fs.Append]))
  use index_info <- try(file.file_info(index))
  Ok(IndexFile(index_pid, index_info.size, index))
}

pub fn close(index_file: IndexFile) -> Result(Nil, file.Reason) {
  let assert Ok(_) = fs.close(index_file.handler)
  Ok(Nil)
}

pub fn add_event(
  index_file: IndexFile,
  event_size: Int,
  file_id: Int,
) -> #(Index, IndexFile) {
  let #(index_content, index) = case index_file.size {
    0 -> {
      let index = Index(0, event_size, file_id)
      #(<<0:48, event_size:32, file_id:128>>, index)
    }
    _ -> {
      let assert Ok(_) = fs.position(index_file.handler, fs.Eof(-26))
      let assert read.Ok(<<offset:48, prev_size:32, file_id:128>>) =
        fs.read(index_file.handler, 26)
      let offset = offset + prev_size
      let index = Index(offset, event_size, file_id)
      #(<<offset:48, event_size:32, file_id:128>>, index)
    }
  }
  let assert Ok(_) = fs.write(index_file.handler, index_content)
  let index_file = IndexFile(..index_file, size: index_file.size + 26)
  #(index, index_file)
}

pub fn get_num_of_events(index_file: IndexFile) -> Int {
  let assert Ok(result) = int.divide(index_file.size, 26)
  result
}

pub fn set_index_pos(
  index_file: IndexFile,
  index: Int,
) -> Result(Int, file.Reason) {
  fs.position(index_file.handler, fs.Bof(index * 26))
}

pub fn get_next_index(index_file: IndexFile) -> Result(Index, file.Reason) {
  case fs.read(index_file.handler, 26) {
    read.Ok(<<offset:48, size:32, file_id:128>>) ->
      Ok(Index(offset, size, file_id))
    read.Eof -> Error(file.Espipe)
    _ -> Error(file.Einval)
  }
}

pub fn get_index(
  index_file: IndexFile,
  index: Int,
) -> Result(Index, file.Reason) {
  case get_num_of_events(index_file) > index {
    True -> {
      let assert Ok(_) = set_index_pos(index_file, index)
      get_next_index(index_file)
    }
    False -> Error(file.Einval)
  }
}
