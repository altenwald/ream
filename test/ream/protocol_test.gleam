import ream/protocol
import ream/protocol/message

pub fn parsing_ping_and_pong_test() {
  let assert message.Ping = protocol.parse("PING")
  let assert message.Pong = protocol.parse("PONG")
  let assert message.EventSubscribe("users", 25) =
    protocol.parse("EVENT SUBSCRIBE users 25")
}
