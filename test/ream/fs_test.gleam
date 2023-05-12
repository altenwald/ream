import glacier
import glacier/should
import gleam/erlang/file.{Ebadf, Enoent}
import ream/fs
import ream/fs/read

pub fn main() {
  glacier.main()
}

pub fn open_test() {
  fs.open("LICENSE", [fs.Read])
  |> should.be_ok()
  |> fs.close()
  |> should.be_ok()

  fs.open("no-such-file.txt", [fs.Read])
  |> should.be_error()
  |> should.equal(Enoent)
}

pub fn read_test() {
  let file =
    fs.open("LICENSE", [fs.Read])
    |> should.be_ok()

  file
  |> fs.read(47)
  |> should.equal(read.Ok("                                 Apache License"))

  file
  |> fs.close()
  |> should.be_ok()
}

pub fn position_test() {
  let file =
    fs.open("LICENSE", [fs.Read, fs.Write])
    |> should.be_ok()

  file
  |> fs.position(fs.Bof(33))
  |> should.be_ok()

  file
  |> fs.write("a")
  |> should.be_ok()

  file
  |> fs.position(fs.Cur(-1))
  |> should.be_ok()

  file
  |> fs.read(14)
  |> should.equal(read.Ok("apache License"))

  file
  |> fs.position(fs.Cur(-14))
  |> should.be_ok()

  file
  |> fs.write("A")
  |> should.be_ok()

  file
  |> fs.position(fs.Cur(-1))
  |> should.be_ok()

  file
  |> fs.read(14)
  |> should.equal(read.Ok("Apache License"))

  file
  |> fs.close()
  |> should.be_ok()
}

pub fn write_test() {
  let file =
    fs.open("tmp.txt", [fs.Write])
    |> should.be_ok()

  file
  |> fs.write("Hello, world!")
  |> should.be_ok()

  file
  |> fs.close()
  |> should.be_ok()

  fs.open("tmp.txt", [fs.Read])
  |> should.be_ok()
  |> fs.read(13)
  |> should.equal(read.Ok("Hello, world!"))

  let file =
    fs.open("LICENSE", [fs.Read])
    |> should.be_ok()

  file
  |> fs.position(fs.Bof(33))
  |> should.be_ok()

  file
  |> fs.write("a")
  |> should.be_error()
  |> should.equal(Ebadf)

  file.delete("tmp.txt")
  |> should.be_ok()
}
