import glacier
import glacier/should

pub fn main() {
  glacier.main()
}

// glacier test functions end in `_test`
pub fn hello_world_test() {
  1
  |> should.equal(1)
}
