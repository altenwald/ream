//// File module is including some functions that are not in the Erlang file module
//// provided by Gleam.
////
//// Because the Erlang functions hasn't an homogeneous API, we are providing a
//// wrapper to make it easier to use and convert the result to a Gleam type. That's
//// why we are using the `storage/file/read`, `storage/file/write` and
//// `storage/file/close` modules.

import gleam/erlang/process.{Pid}
import gleam/erlang/file
import ream/storage/file/read
import ream/storage/file/close
import ream/storage/file/write

/// Helper for the EncodingType type
pub type Endian {
  Big
  Little
}

/// Helper for the Mode type
pub type EncodingType {
  Unicode
  Utf8
  Utf16(Endian)
  Utf32(Endian)
  Latin1
}

/// Mode is a list of options for open and create files.
pub type Mode {
  Read
  Write
  Append
  Exclusive
  Raw
  Binary
  DelayedWrite(size: Int, delay: Int)
  ReadAhead(size: Int)
  Compressed
  CompressedOne
  Encoding(encoding: EncodingType)
  Ram
  Sync
  Directory
}

/// Location is a type used to specify the position in a file.
pub type Location {
  Bof(Int)
  Cur(Int)
  Eof(Int)
}

/// The open function opens a file in the given mode. The function is returning
/// a Pid that can be used to read and write in the file.
pub fn open(filename: String, mode: List(Mode)) -> Result(Pid, file.Reason) {
  do_open(filename, [Binary, ..mode])
}

external fn do_open(
  filename: String,
  mode: List(Mode),
) -> Result(Pid, file.Reason) =
  "file" "open"

/// The read function is reading the given number of bytes from the file.
/// The function is returning a tuple with the result of the operation and the
/// data read.
pub external fn read(io_device: Pid, bytes: Int) -> read.Result =
  "file" "read"

/// The close function is closing the file. The function is returning true
/// if the file was closed successfully.
pub fn close(io_device: Pid) -> Result(Bool, file.Reason) {
  case do_close(io_device) {
    close.Ok -> Ok(True)
    close.Error(reason) -> Error(reason)
  }
}

external fn do_close(io_device: Pid) -> close.Result =
  "file" "close"

/// The write function is writing the given data in the file.
pub fn write(io_device: Pid, data: BitString) -> Result(Bool, file.Reason) {
  case do_write(io_device, data) {
    write.Ok -> Ok(True)
    write.Error(reason) -> Error(reason)
  }
}

external fn do_write(io_device: Pid, data: BitString) -> write.Result =
  "file" "write"

/// The dirname function is returning the directory name of the given filename.
/// As an example, if the filename is `/tmp/foo/bar.txt`, the function will
/// return `/tmp/foo`.
pub external fn dirname(filename: String) -> String =
  "filename" "dirname"

/// The basename function is returning the base name of the given filename.
/// As an example, if the filename is `/tmp/foo/bar.txt`, the function will
/// return `bar.txt`.
pub external fn basename(filename: String) -> String =
  "filename" "basename"

/// The join function is joining the given parts to create a filename.
/// As an example, if the parts are `["/tmp", "foo", "bar.txt"]`, the function
/// will return `/tmp/foo/bar.txt`.
pub external fn join(parts: List(String)) -> String =
  "filename" "join"

/// The position function is returning the current position in the file.
pub external fn position(
  io_device: Pid,
  location: Location,
) -> Result(Int, file.Reason) =
  "file" "position"

/// The recursive make directory function let us to create a directory and
/// all the parent directories if they don't exist.
pub fn recursive_make_directory(path: String) -> Result(Bool, file.Reason) {
  case file.is_directory(path) {
    Error(file.Enoent) -> {
      let prev_dir = dirname(path)
      let assert Ok(True) = recursive_make_directory(prev_dir)
      let assert Ok(_) = file.make_directory(path)
      Ok(True)
    }
    Error(file.Eexist) -> Ok(True)
    Ok(True) -> Ok(True)
    _ -> Error(file.Einval)
  }
}
