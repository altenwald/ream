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
}

pub fn parsing_event_test() {
  let assert Ok(message.Event(
    "users",
    1001,
    16,
    <<"{\"name\":\"peter\"}":utf8>>,
  )) = parser.process("EVENT users 1001 16\n{\"name\":\"peter\"}")
  let assert Ok(message.EventSubscribe("users", 25)) =
    parser.process("EVENT SUBSCRIBE users 25")
  let assert Ok(message.EventSubscribed("users", 1, 1001)) =
    parser.process("EVENT SUBSCRIBED users 1 1001")
  let assert Ok(message.EventSubscribed("users", 0, 0)) =
    parser.process("EVENT SUBSCRIBED users EMPTY")
  let assert Ok(message.EventUnsubscribe("users")) =
    parser.process("EVENT UNSUBSCRIBE users")
  let assert Ok(message.EventUnsubscribed("users")) =
    parser.process("EVENT UNSUBSCRIBED users")
  let assert Ok(message.EventList) = parser.process("EVENT LIST")
  let assert Ok(message.EventListed(["users", "accounts"])) =
    parser.process("EVENT LISTED 2 users accounts")
  let assert Ok(message.EventPublish(
    "emails",
    14,
    <<"{\"name\":\"tom\"}":utf8>>,
  )) = parser.process("EVENT PUBLISH emails 14\n{\"name\":\"tom\"}")
  let assert Ok(message.EventPublished("emails", 1002)) =
    parser.process("EVENT PUBLISHED emails 1002")
  let assert Ok(message.EventNonPublished("emails", "Invalid message")) =
    parser.process("EVENT NON PUBLISHED emails \"Invalid message\"")
  let assert Ok(message.EventRemove("emails")) =
    parser.process("EVENT REMOVE emails")
  let assert Ok(message.EventRemoved("emails")) =
    parser.process("EVENT REMOVED emails")
}

pub fn parsing_aggregate_test() {
  let assert Ok(message.AggregateSet(
    "emails",
    52_928_765_732_754_145_749_045_094_502_427_735_649,
    15,
    <<"{\"emails\":1002}":utf8>>,
  )) =
    parser.process(
      "AGGREGATE SET emails \"27d1b5a0-c54f-4664-a549-b876b0bb3661\" 15\n{\"emails\":1002}",
    )
  let assert Ok(message.AggregateSet(
    "emails",
    52_928_765_732_754_145_749_045_094_502_427_735_649,
    15,
    <<"{\"emails\":1002}":utf8>>,
  )) =
    parser.process(
      "AGGREGATE SET emails 52928765732754145749045094502427735649 15\n{\"emails\":1002}",
    )
  let assert Ok(message.AggregateSetDone(
    "emails",
    52_928_765_732_754_145_749_045_094_502_427_735_649,
  )) =
    parser.process(
      "AGGREGATE SET DONE emails \"27d1b5a0-c54f-4664-a549-b876b0bb3661\"",
    )
}

pub fn wrong_parsing_test() {
  let assert Error("Invalid message, cannot parse (dead end)") =
    parser.process("PGUEA")
  let assert Error("Expected valid name") =
    parser.process("EVENT SUBSCRIBE 99 25")
  let assert Error("Invalid message, cannot parse (dead end)") =
    parser.process("EVENT LISTED 5 users accounts")
  let assert Error("Invalid message, provided size is different from content") =
    parser.process("EVENT users 1001 200\n{\"name\":")
}
