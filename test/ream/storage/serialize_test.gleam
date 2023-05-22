import glacier
import ream/storage/serialize
import gleam/dynamic

pub fn main() {
  glacier.main()
}

pub fn serialize_test() {
  let data =
    [1, 2, 3]
    |> dynamic.from()

  let bitstring = serialize.encode(data)
  let assert Ok([1, 2, 3]) =
    bitstring
    |> serialize.decode()
    |> dynamic.list(of: dynamic.int)
}
