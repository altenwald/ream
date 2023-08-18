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
  Name(String)
  Integer(Int)
}

pub fn parse(text: String) -> message.Message {
  let lexer =
    lexer.simple([
      lexer.token("PING", Ping),
      lexer.token("PONG", Pong),
      lexer.token("EVENT", Event),
      lexer.token("SUBSCRIBED", Subscribed),
      lexer.token("SUBSCRIBE", Subscribe),
      lexer.token("PUBLISHED", Published),
      lexer.token("PUBLISH", Publish),
      lexer.token("NON", Non),
      lexer.identifier("[A-Za-z]", "[A-Za-z0-9_]", set.new(), Name),
      lexer.int(Integer),
      lexer.whitespace(Nil)
      |> lexer.ignore(),
    ])

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
        use _ <- nibble.do(nibble.token(Subscribe))
        use name <- nibble.do(get_name())
        use id <- nibble.do(get_id())
        nibble.return(message.EventSubscribe(name, id))
      },
    ])
  }

  let assert Ok(tokens) = lexer.run(text, lexer)
  let assert Ok(command) = nibble.run(tokens, parser)
  command
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
