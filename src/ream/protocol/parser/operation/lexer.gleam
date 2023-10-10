import gleam/set
import nibble/lexer

pub type Token {
  All
  And
  Or
  Not
  In
  Contains
  Regex
  Equal
  NotEqual
  GreaterThan
  LesserThan
  GreaterOrEqualThan
  LesserOrEqualThan
  LeftBracket
  LeftCurlyBrace
  LeftParens
  RightBracket
  RightCurlyBrace
  RightParens
  Coma
  Null
  True
  False
  Name(String)
  Float(Float)
  Integer(Int)
  String(String)
}

pub fn tokenize(text: String) {
  let keywords =
    set.from_list([
      "AND", "OR", "NOT", "IN", "CONTAINS", "TRUE", "FALSE", "NULL",
    ])
  let lexer =
    lexer.simple([
      lexer.keyword("AND", "[^\\w\\d]", And),
      lexer.keyword("OR", "[^\\w\\d]", Or),
      lexer.keyword("NOT", "[^\\w\\d]", Or),
      lexer.keyword("IN", "[^\\w\\d]", In),
      lexer.keyword("CONTAINS", "[^\\w\\d]", Contains),
      lexer.keyword("TRUE", "[^\\w\\d]", True),
      lexer.keyword("FALSE", "[^\\w\\d]", False),
      lexer.keyword("NULL", "[^\\w\\d]", Null),
      lexer.token("!=", NotEqual),
      lexer.token("(", LeftParens),
      lexer.token(")", RightParens),
      lexer.token(",", Coma),
      lexer.symbol(">", "[^=]", GreaterThan),
      lexer.symbol("<", "[^=]", LesserThan),
      lexer.symbol("=", "[^~]", Equal),
      lexer.token(">=", GreaterOrEqualThan),
      lexer.token("<=", LesserOrEqualThan),
      lexer.token("=~", Regex),
      lexer.identifier("[A-Za-z]", "[A-Za-z0-9_]", keywords, Name),
      lexer.float(Float),
      lexer.int(Integer),
      lexer.string("\"", String),
      lexer.whitespace(Nil)
      |> lexer.ignore(),
    ])

  lexer.run(text, lexer)
}
