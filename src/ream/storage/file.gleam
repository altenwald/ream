//// File module is including some functions that are not in the Erlang file module
//// provided by Gleam.
////
//// Because the Erlang functions hasn't an homogeneous API, we are providing a
//// wrapper to make it easier to use and convert the result to a Gleam type. That's
//// why we are using the `storage/file/read`, `storage/file/write` and
//// `storage/file/close` modules.

import gleam/erlang/process.{Pid, Subject}
import gleam/erlang/file
import gleam/otp/actor.{InitFailed}
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

pub type Message {
  Shutdown(from: Subject(Result(Bool, file.Reason)))
  ReadMessage(from: Subject(read.Result), offset: Location, size: Int)
  WriteMessage(
    from: Subject(Result(Bool, file.Reason)),
    offset: Location,
    data: BitString,
  )
}

pub type State {
  State(pid: Pid)
}

/// The open function opens a file in the given mode. The function is returning
/// a Pid that can be used to read and write in the file.
pub fn open(
  filename: String,
  mode: List(Mode),
) -> Result(Subject(Message), file.Reason) {
  case
    actor.start_spec(actor.Spec(
      init: fn() { init(filename, mode) },
      init_timeout: 5000,
      loop: handle_message,
    ))
  {
    Ok(subject) -> Ok(subject)
    Error(actor.InitTimeout) -> Error(file.Ebusy)
    Error(actor.InitFailed(_reason)) -> Error(file.Enoent)
    Error(actor.InitCrashed(_reason)) -> Error(file.Einval)
  }
}

fn init(filename: String, mode: List(Mode)) -> actor.InitResult(State, Message) {
  case file_open(filename, [Binary, Raw, ..mode]) {
    Ok(pid) -> actor.Ready(State(pid), process.new_selector())
    Error(_reason) -> actor.Failed("cannot open file " <> filename)
  }
}

@external(erlang, "file", "open")
fn file_open(filename filename: String, mode mode: List(Mode)) -> Result(
  Pid,
  file.Reason,
)

pub fn handle_message(message: Message, state: State) -> actor.Next(State) {
  case message {
    Shutdown(from) -> {
      let result = do_close(state.pid)
      process.send(from, result)
      actor.Stop(process.Normal)
    }

    ReadMessage(from, offset, bytes) -> {
      let result = do_read(state.pid, offset, bytes)
      process.send(from, result)
      actor.Continue(state)
    }

    WriteMessage(from, offset, data) -> {
      let result = do_write(state.pid, offset, data)
      process.send(from, result)
      actor.Continue(state)
    }
  }
}

/// The close function is closing the file. The function is returning true
/// if the file was closed successfully.
pub fn close(io_device: Subject(Message)) -> Result(Bool, file.Reason) {
  process.call(io_device, Shutdown, 5000)
}

fn do_close(io_device: Pid) -> Result(Bool, file.Reason) {
  case file_close(io_device) {
    close.Ok -> Ok(True)
    close.Error(reason) -> Error(reason)
  }
}

@external(erlang, "file", "close")
fn file_close(io_device io_device: Pid) -> close.Result

/// The read function is reading the given number of bytes from the file.
/// The function is returning a tuple with the result of the operation and the
/// data read.
pub fn read(
  io_device: Subject(Message),
  offset: Location,
  bytes: Int,
) -> read.Result {
  actor.call(io_device, ReadMessage(_, offset, bytes), 5000)
}

fn do_read(io_device: Pid, offset: Location, bytes: Int) -> read.Result {
  case offset {
    Cur(0) -> file_read(io_device, bytes)
    _ -> {
      case position(io_device, offset) {
        Ok(_) -> file_read(io_device, bytes)
        Error(reason) -> read.Error(reason)
      }
    }
  }
}

@external(erlang, "file", "read")
fn file_read(io_device io_device: Pid, bytes bytes: Int) -> read.Result

/// The write function is writing the given data in the file.
pub fn write(
  io_device: Subject(Message),
  offset: Location,
  data: BitString,
) -> Result(Bool, file.Reason) {
  actor.call(io_device, WriteMessage(_, offset, data), 5000)
}

fn do_write(
  io_device: Pid,
  offset: Location,
  data: BitString,
) -> Result(Bool, file.Reason) {
  case offset {
    Cur(0) -> {
      case file_write(io_device, data) {
        write.Ok -> Ok(True)
        write.Error(reason) -> Error(reason)
      }
    }
    _ -> {
      case position(io_device, offset) {
        Ok(_) -> do_write(io_device, Cur(0), data)
        Error(reason) -> Error(reason)
      }
    }
  }
}

@external(erlang, "file", "write")
fn file_write(io_device io_device: Pid, data data: BitString) -> write.Result

/// The dirname function is returning the directory name of the given filename.
/// As an example, if the filename is `/tmp/foo/bar.txt`, the function will
/// return `/tmp/foo`.
@external(erlang, "filename", "dirname")
pub fn dirname(filename filename: String) -> String

/// The basename function is returning the base name of the given filename.
/// As an example, if the filename is `/tmp/foo/bar.txt`, the function will
/// return `bar.txt`.
@external(erlang, "filename", "basename")
pub fn basename(filename filename: String) -> String

/// The join function is joining the given parts to create a filename.
/// As an example, if the parts are `["/tmp", "foo", "bar.txt"]`, the function
/// will return `/tmp/foo/bar.txt`.
@external(erlang, "filename", "join")
pub fn join(parts parts: List(String)) -> String

/// The position function is returning the current position in the file.
@external(erlang, "file", "position")
fn position(io_device io_device: Pid, location location: Location) -> Result(
  Int,
  file.Reason,
)

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
