import gleam/erlang/process.{Pid}
import gleam/erlang/file
import ream/storage/fs/read
import ream/storage/fs/close
import ream/storage/fs/write

pub type Endian {
  Big
  Little
}

pub type EncodingType {
  Unicode
  Utf8
  Utf16(Endian)
  Utf32(Endian)
  Latin1
}

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

pub type Location {
  Bof(Int)
  Cur(Int)
  Eof(Int)
}

pub fn open(filename: String, mode: List(Mode)) -> Result(Pid, file.Reason) {
  do_open(filename, [Binary, ..mode])
}

pub external fn do_open(
  filename: String,
  mode: List(Mode),
) -> Result(Pid, file.Reason) =
  "file" "open"

pub external fn read(io_device: Pid, bytes: Int) -> read.Result =
  "file" "read"

pub fn close(io_device: Pid) -> Result(Bool, file.Reason) {
  case do_close(io_device) {
    close.Ok -> Ok(True)
    close.Error(reason) -> Error(reason)
  }
}

external fn do_close(io_device: Pid) -> close.Result =
  "file" "close"

pub fn write(io_device: Pid, data: String) -> Result(Bool, file.Reason) {
  case do_write(io_device, data) {
    write.Ok -> Ok(True)
    write.Error(reason) -> Error(reason)
  }
}

external fn do_write(io_device: Pid, data: String) -> write.Result =
  "file" "write"

pub external fn position(
  io_device: Pid,
  location: Location,
) -> Result(Int, file.Reason) =
  "file" "position"
