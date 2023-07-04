import gleam/bit_string
import gleam/erlang/file.{Ebadf, Enoent}
import ream/storage/file as fs
import ream/storage/file/read.{Eof, Ok as ReadOk}

pub fn open_and_close_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Ok(True) = fs.close(file)
}

pub fn open_error_test() {
  let assert Error(Enoent) = fs.open("no-such-file.txt", [fs.Read])
}

pub fn read_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert ReadOk(bs) = fs.read(file, fs.Cur(33), 14)
  let assert Ok("Apache License") = bit_string.to_string(bs)
  let assert Ok(True) = fs.close(file)
}

pub fn read_eof_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Eof = fs.read(file, fs.Eof(0), 1024)
  let assert Ok(True) = fs.close(file)
}

pub fn write_and_position_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read, fs.Write])
  let assert Ok(True) = fs.write(file, fs.Bof(33), bit_string.from_string("a"))
  let assert ReadOk(bs) = fs.read(file, fs.Cur(-1), 14)
  let assert Ok("apache License") = bit_string.to_string(bs)
  let assert Ok(True) = fs.write(file, fs.Cur(-14), bit_string.from_string("A"))
  let assert ReadOk(bs) = fs.read(file, fs.Cur(-1), 14)
  let assert Ok("Apache License") = bit_string.to_string(bs)
  let assert Ok(True) = fs.close(file)
}

pub fn write_test() {
  let assert Ok(file) = fs.open("tmp.txt", [fs.Write])
  let assert Ok(True) =
    fs.write(file, fs.Cur(0), bit_string.from_string("Hello, world!"))
  let assert Ok(True) = fs.close(file)

  let assert Ok(file) = fs.open("tmp.txt", [fs.Read])
  let assert ReadOk(bs) = fs.read(file, fs.Cur(0), 13)
  let assert Ok("Hello, world!") = bit_string.to_string(bs)
  let assert Ok(True) = fs.close(file)
  let assert Ok(Nil) = file.delete("tmp.txt")
}

pub fn write_error_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Error(Ebadf) =
    fs.write(file, fs.Cur(0), bit_string.from_string("X"))
  let assert Ok(True) = fs.close(file)
}
