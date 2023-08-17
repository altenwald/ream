import gleam/order.{Eq, Gt, Lt}
import ream/storage/schema/data_type.{BitString,
  Boolean, Float, Integer, String}

pub fn compare_test() {
  let assert Gt = data_type.compare(Integer(10), Integer(5))
  let assert Eq = data_type.compare(Integer(5), Float(5.5))
  let assert Eq = data_type.compare(Float(5.0), Integer(5))
  let assert Lt = data_type.compare(String("HELLO"), String("hello"))
  let assert Lt = data_type.compare(Boolean(False), Boolean(True))
  let assert Eq =
    data_type.compare(BitString(<<"hello":utf8>>), String("hello"))
}
