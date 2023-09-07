import gleam/bit_string
import gleam/list
import gleam/result.{try}
import gleam/set
import gleam/string
import nibble
import nibble/lexer
import ream/protocol/message
import ream/uuid

pub type Token {
  Ping
  Pong
  Event
  Subscribe
  Subscribed
  Empty
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
  StringToken(String)
}

pub fn tokenize(text: String) {
  let keywords =
    set.from_list([
      "PING", "PONG", "SUBSCRIBE", "SUBSCRIBED", "EMPTY", "PUBLISH", "PUBLISHED",
      "NON", "UNSUBSCRIBE", "UNSUBSCRIBED", "LIST", "LISTED", "REMOVE",
      "REMOVED", "AGGREGATE", "SET", "DONE", "GET", "GOT", "PROJECTION",
      "CREATE", "CREATED", "DROP", "DROPPED", "SELECT", "SELECTED", "UPDATE",
      "UPDATED", "DELETE", "DELETED", "EVENT",
    ])

  let lexer =
    lexer.simple([
      lexer.keyword("PING", "[^\\w\\d]", Ping),
      lexer.keyword("PONG", "[^\\w\\d]", Pong),
      lexer.keyword("EVENT", "[^\\w\\d]", Event),
      lexer.keyword("SUBSCRIBE", "[^\\w\\d]", Subscribe),
      lexer.keyword("SUBSCRIBED", "[^\\w\\d]", Subscribed),
      lexer.keyword("EMPTY", "[^\\w\\d]", Empty),
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
      lexer.string("\"", StringToken),
      lexer.whitespace(Nil)
      |> lexer.ignore(),
    ])

  lexer.run(text, lexer)
}

fn parse_event() {
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
        use #(first_id, latest_id) <- nibble.do(nibble.one_of([
          {
            use first_id <- nibble.do(get_id())
            use latest_id <- nibble.do(get_id())
            nibble.return(#(first_id, latest_id))
          },
          {
            use _ <- nibble.do(nibble.token(Empty))
            nibble.return(#(0, 0))
          },
        ]))
        nibble.return(message.EventSubscribed(name, first_id, latest_id))
      },
      {
        use _ <- nibble.do(nibble.token(Publish))
        use name <- nibble.do(get_name())
        use size <- nibble.do(get_size())
        nibble.return(message.EventPublish(name, size, <<>>))
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
        use size <- nibble.do(get_size())
        nibble.return(message.Event(name, id, size, <<>>))
      },
    ])
  }
}

fn parse_aggregate() {
  {
    use _ <- nibble.do(nibble.token(Aggregate))
    nibble.one_of([
      {
        use _ <- nibble.do(nibble.token(Set))
        nibble.one_of([
          {
            use _ <- nibble.do(nibble.token(Done))
            use name <- nibble.do(get_name())
            use uuid <- nibble.do(get_uuid())
            nibble.return(message.AggregateSetDone(name, uuid))
          },
          {
            use name <- nibble.do(get_name())
            use uuid <- nibble.do(get_uuid())
            use size <- nibble.do(get_size())
            nibble.return(message.AggregateSet(name, uuid, size, <<>>))
          },
        ])
      },
    ])
  }
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
      parse_event(),
      parse_aggregate(),
    ])
  }

  case nibble.run(tokens, parser) {
    Ok(command) -> Ok(command)
    Error(errors) -> Error(dead_end_filter(errors))
  }
}

fn get_content(text: String, size: Int) -> Result(BitString, String) {
  case string.byte_size(text) == size {
    True -> {
      case
        text
        |> bit_string.from_string()
        |> bit_string.slice(0, size)
      {
        Ok(text) -> Ok(text)
        Error(Nil) ->
          Error("Invalid message, provided size is larger than content")
      }
    }
    False -> Error("Invalid message, provided size is different from content")
  }
}

fn adjust(
  parsed: message.Message,
  text: String,
) -> Result(message.Message, String) {
  case parsed {
    message.Event(name, id, size, _content) -> {
      use content <- try(get_content(text, size))
      Ok(message.Event(name, id, size, content))
    }
    message.EventPublish(name, size, _content) -> {
      use content <- try(get_content(text, size))
      Ok(message.EventPublish(name, size, content))
    }
    message.AggregateSet(name, id, size, _content) -> {
      use content <- try(get_content(text, size))
      Ok(message.AggregateSet(name, id, size, content))
    }
    _ -> Ok(parsed)
  }
}

pub fn process(text: String) -> Result(message.Message, String) {
  let #(line, text) = case string.split_once(text, "\n") {
    Ok(result) -> result
    Error(Nil) -> #(text, "")
  }
  case tokenize(line) {
    Ok(tokens) -> {
      use parsed <- try(parse(tokens))
      adjust(parsed, text)
    }
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
    _ -> "Invalid message, cannot parse (dead end)"
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

fn get_uuid() {
  use uuid <- nibble.do(nibble.any())

  case uuid {
    Integer(i) -> nibble.return(i)
    StringToken(s) -> {
      let b = uuid.from_string(s)
      nibble.return(uuid.to_int(b))
    }
    _error -> nibble.fail("Expected a valid UUID")
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
