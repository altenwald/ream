import gleeunit
import gleam/erlang/file.{Ebadf, Enoent}
import ream/storage/fs
import ream/storage/fs/read.{Eof, Ok as ReadOk}

pub fn main() {
  gleeunit.main()
}

pub fn open_and_close_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Ok(True) = fs.close(file)
}

pub fn open_error_test() {
  let assert Error(Enoent) = fs.open("no-such-file.txt", [fs.Read])
}

pub fn read_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert ReadOk("                                 Apache License") =
    fs.read(file, 47)
  let assert Ok(True) = fs.close(file)
}

pub fn read_eof_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Ok(_) = fs.position(file, fs.Eof(0))
  let assert Eof = fs.read(file, 1024)
  let assert Ok(True) = fs.close(file)
}

pub fn write_and_position_ok_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read, fs.Write])
  let assert Ok(33) = fs.position(file, fs.Bof(33))
  let assert Ok(True) = fs.write(file, "a")
  let assert Ok(33) = fs.position(file, fs.Cur(-1))
  let assert ReadOk("apache License") = fs.read(file, 14)
  let assert Ok(33) = fs.position(file, fs.Cur(-14))
  let assert Ok(True) = fs.write(file, "A")
  let assert Ok(33) = fs.position(file, fs.Cur(-1))
  let assert ReadOk("Apache License") = fs.read(file, 14)
  let assert Ok(True) = fs.close(file)
}

pub fn write_test() {
  let assert Ok(file) = fs.open("tmp.txt", [fs.Write])
  let assert Ok(True) = fs.write(file, "Hello, world!")
  let assert Ok(True) = fs.close(file)

  let assert Ok(file) = fs.open("tmp.txt", [fs.Read])
  let assert ReadOk("Hello, world!") = fs.read(file, 13)
  let assert Ok(True) = fs.close(file)
  let assert Ok(Nil) = file.delete("tmp.txt")
}

pub fn write_error_test() {
  let assert Ok(file) = fs.open("LICENSE", [fs.Read])
  let assert Error(Ebadf) = fs.write(file, "X")
  let assert Ok(True) = fs.close(file)
}
