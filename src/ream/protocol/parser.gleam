import gleam/function
import gleam/int
import gleam/list
import gleam/set
import nibble
import nibble/lexer
import ream/protocol/message

pub type Token {
  Ping
  Pong
  Event
  Subscribe
  Subscribed
  Publish
  Published
  Non
  Unsubscribe
  Unsubscribed
  List
  Listed
  Remove
  Removed
  Aggregate
  Set
  Done
  Get
  Got
  Projection
  Create
  Created
  Drop
  Dropped
  Select
  Selected
  Update
  Updated
  Delete
  Deleted
  Name(String)
  Integer(Int)
  Colon
  Comma
  FalseToken
  LBrace
  LBracket
  NullToken
  Number(Float)
  RBrace
  RBracket
  StringToken(String)
  TrueToken
}

pub fn tokenize(text: String) {
  let keywords = set.from_list(["PING", "PONG", "SUBSCRIBE", "SUBSCRIBED"])

  let lexer =
    lexer.simple([
      lexer.keyword("PING", "[^\\w\\d]", Ping),
      lexer.keyword("PONG", "[^\\w\\d]", Pong),
      lexer.keyword("EVENT", "[^\\w\\d]", Event),
      lexer.keyword("SUBSCRIBE", "[^\\w\\d]", Subscribe),
      lexer.keyword("SUBSCRIBED", "[^\\w\\d]", Subscribed),
      lexer.keyword("PUBLISH", "[^\\w\\d]", Publish),
      lexer.keyword("PUBLISHED", "[^\\w\\d]", Published),
      lexer.keyword("NON", "[^\\w\\d]", Non),
      lexer.keyword("UNSUBSCRIBE", "[^\\w\\d]", Unsubscribe),
      lexer.keyword("UNSUBSCRIBED", "[^\\w\\d]", Unsubscribed),
      lexer.keyword("LIST", "[^\\w\\d]", List),
      lexer.keyword("LISTED", "[^\\w\\d]", Listed),
      lexer.keyword("REMOVE", "[^\\w\\d]", Remove),
      lexer.keyword("REMOVED", "[^\\w\\d]", Removed),
      lexer.keyword("AGGREGATE", "[^\\w\\d]", Aggregate),
      lexer.keyword("SET", "[^\\w\\d]", Set),
      lexer.keyword("DONE", "[^\\w\\d]", Done),
      lexer.keyword("GET", "[^\\w\\d]", Get),
      lexer.keyword("GOT", "[^\\w\\d]", Got),
      lexer.keyword("PROJECTION", "[^\\w\\d]", Projection),
      lexer.keyword("CREATE", "[^\\w\\d]", Create),
      lexer.keyword("CREATED", "[^\\w\\d]", Created),
      lexer.keyword("DROP", "[^\\w\\d]", Drop),
      lexer.keyword("DROPPED", "[^\\w\\d]", Dropped),
      lexer.keyword("SELECT", "[^\\w\\d]", Select),
      lexer.keyword("SELECTED", "[^\\w\\d]", Selected),
      lexer.keyword("UPDATE", "[^\\w\\d]", Update),
      lexer.keyword("UPDATED", "[^\\w\\d]", Updated),
      lexer.keyword("DELETE", "[^\\w\\d]", Delete),
      lexer.keyword("DELETED", "[^\\w\\d]", Deleted),
      lexer.identifier("[A-Za-z]", "[A-Za-z0-9_]", keywords, Name),
      lexer.int(Integer),
      lexer.number(function.compose(int.to_float, Number), Number),
      lexer.token(":", Colon),
      lexer.token(",", Comma),
      lexer.token("false", FalseToken),
      lexer.token("{", LBrace),
      lexer.token("[", LBracket),
      lexer.token("null", NullToken),
      lexer.token("true", TrueToken),
      lexer.token("}", RBrace),
      lexer.token("]", RBracket),
      lexer.string("\"", StringToken),
      lexer.whitespace(Nil)
      |> lexer.ignore(),
    ])

  lexer.run(text, lexer)
}

pub fn parse(tokens) -> Result(message.Message, String) {
  let parser = {
    nibble.one_of([
      {
        use _ <- nibble.do(nibble.token(Ping))
        nibble.return(message.Ping)
      },
      {
        use _ <- nibble.do(nibble.token(Pong))
        nibble.return(message.Pong)
      },
      {
        use _ <- nibble.do(nibble.token(Event))
        nibble.one_of([
          {
            use _ <- nibble.do(nibble.token(Subscribe))
            use name <- nibble.do(get_name())
            use id <- nibble.do(get_id())
            nibble.return(message.EventSubscribe(name, id))
          },
          {
            use _ <- nibble.do(nibble.token(Subscribed))
            use name <- nibble.do(get_name())
            use first_id <- nibble.do(get_id())
            use latest_id <- nibble.do(get_id())
            nibble.return(message.EventSubscribed(name, first_id, latest_id))
          },
          {
            use _ <- nibble.do(nibble.token(Publish))
            use name <- nibble.do(get_name())
            use json <- nibble.do(json_parser())
            nibble.return(message.EventPublish(name, json))
          },
          {
            use _ <- nibble.do(nibble.token(Published))
            use name <- nibble.do(get_name())
            use id <- nibble.do(get_id())
            nibble.return(message.EventPublished(name, id))
          },
          {
            use _ <- nibble.do(nibble.token(Non))
            use _ <- nibble.do(nibble.token(Published))
            use name <- nibble.do(get_name())
            use reason <- nibble.do(get_reason())
            nibble.return(message.EventNonPublished(name, reason))
          },
          {
            use _ <- nibble.do(nibble.token(Unsubscribe))
            use name <- nibble.do(get_name())
            nibble.return(message.EventUnsubscribe(name))
          },
          {
            use _ <- nibble.do(nibble.token(Unsubscribed))
            use name <- nibble.do(get_name())
            nibble.return(message.EventUnsubscribed(name))
          },
          {
            use _ <- nibble.do(nibble.token(List))
            nibble.return(message.EventList)
          },
          {
            use _ <- nibble.do(nibble.token(Listed))
            use size <- nibble.do(get_size())
            use names <- nibble.do(nibble.take_exactly(get_name(), size))
            nibble.return(message.EventListed(names))
          },
          {
            use _ <- nibble.do(nibble.token(Remove))
            use name <- nibble.do(get_name())
            nibble.return(message.EventRemove(name))
          },
          {
            use _ <- nibble.do(nibble.token(Removed))
            use name <- nibble.do(get_name())
            nibble.return(message.EventRemoved(name))
          },
          {
            use name <- nibble.do(get_name())
            use id <- nibble.do(get_id())
            use _size <- nibble.do(get_size())
            use json <- nibble.do(json_parser())
            nibble.return(message.Event(name, id, json))
          },
        ])
      },
    ])
  }

  case nibble.run(tokens, parser) {
    Ok(command) -> Ok(command)
    Error(errors) -> Error(dead_end_filter(errors))
  }
}

pub fn process(text: String) -> Result(message.Message, String) {
  case tokenize(text) {
    Ok(tokens) -> parse(tokens)
    Error(_) -> Error("Cannot parse text")
  }
}

fn dead_end_filter(errors) -> String {
  let error_msgs =
    list.filter_map(
      errors,
      fn(error) {
        case error {
          nibble.DeadEnd(_, nibble.Custom(msg), ..) -> Ok(msg)
          nibble.DeadEnd(..) -> Error(Nil)
        }
      },
    )
  case error_msgs {
    [msg, ..] -> msg
    _ -> "Invalid message"
  }
}

fn get_name() {
  use name <- nibble.do(nibble.any())

  case name {
    Name(name) -> nibble.return(name)
    _ -> nibble.fail("Expected valid name")
  }
}

fn get_id() {
  use id <- nibble.do(nibble.any())

  case id {
    Integer(id) -> nibble.return(id)
    _ -> nibble.fail("Expected valid ID")
  }
}

fn get_size() {
  use size <- nibble.do(nibble.any())

  case size {
    Integer(size) -> nibble.return(size)
    _ -> nibble.fail("Expected a valid size value")
  }
}

fn get_reason() {
  use reason <- nibble.do(nibble.any())

  case reason {
    StringToken(str) -> nibble.return(str)
    _ -> nibble.fail("Expected error reason, no double-quoted text found")
  }
}

// got borrowed from:
// https://github.com/hayleigh-dot-dev/gleam-nibble/blob/v1.0.0-rc.3/test/examples/json_test.gleam
type Context {
  InArray
  InObject
}

fn json_parser() {
  nibble.one_of([
    // Structures
    array_parser()
    |> nibble.in(InArray),
    object_parser()
    |> nibble.in(InObject),
    literal_parser(),
  ])
}

fn array_parser() {
  use _ <- nibble.do(nibble.token(LBracket))
  use elements <- nibble.do(nibble.sequence(
    nibble.lazy(json_parser),
    nibble.token(Comma),
  ))
  use _ <- nibble.do(nibble.token(RBracket))

  nibble.return(message.Array(elements))
}

fn object_parser() {
  use _ <- nibble.do(nibble.token(LBrace))
  use elements <- nibble.do(nibble.sequence(
    nibble.lazy(object_element_parser),
    nibble.token(Comma),
  ))
  use _ <- nibble.do(nibble.token(RBrace))

  nibble.return(message.Object(elements))
}

fn object_element_parser() {
  use key <- nibble.do(nibble.backtrackable({
    use t <- nibble.do(nibble.any())

    case t {
      StringToken(s) -> nibble.return(s)
      _ -> nibble.fail("Expected string object key")
    }
  }))
  use _ <- nibble.do(nibble.token(Colon))
  use value <- nibble.do(nibble.lazy(json_parser))

  nibble.return(#(key, value))
}

fn literal_parser() {
  nibble.backtrackable({
    use t <- nibble.do(nibble.any())

    case t {
      Number(n) -> nibble.return(message.Number(n))
      StringToken(s) -> nibble.return(message.String(s))
      TrueToken -> nibble.return(message.True)
      FalseToken -> nibble.return(message.False)
      NullToken -> nibble.return(message.Null)
      _ -> nibble.fail("Expected a literal value")
    }
  })
}
