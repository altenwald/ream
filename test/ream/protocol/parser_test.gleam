import nibble/lexer
import ream/protocol/parser
import ream/protocol/message

pub fn tokenize_test() {
  let assert Ok([
    lexer.Token(_, "EVENT", parser.Event),
    lexer.Token(_, "SUBSCRIBED", parser.Subscribed),
    lexer.Token(_, "users", parser.Name("users")),
    lexer.Token(_, "1", parser.Integer(1)),
    lexer.Token(_, "1001", parser.Integer(1001)),
  ]) = parser.tokenize("EVENT SUBSCRIBED users 1 1001")
}

pub fn parsing_ping_and_pong_test() {
  let assert Ok(message.Ping) = parser.process("PING")
  let assert Ok(message.Pong) = parser.process("PONG")
  let assert Ok(message.Event(
    "users",
    1001,
    message.Object([#("name", message.String("peter"))]),
  )) = parser.process("EVENT users 1001 15\n{\"name\":\"peter\"}")
  let assert Ok(message.EventSubscribe("users", 25)) =
    parser.process("EVENT SUBSCRIBE users 25")
  let assert Ok(message.EventSubscribed("users", 1, 1001)) =
    parser.process("EVENT SUBSCRIBED users 1 1001")
}

pub fn wrong_parsing_test() {
  let assert Error("Invalid message") = parser.process("PGUEA")
  let assert Error("Expected valid name") =
    parser.process("EVENT SUBSCRIBE 99 25")
}
