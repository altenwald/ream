import nibble/lexer
import ream/protocol/parser/operation
import ream/protocol/parser/operation/lexer as lex
import ream/protocol/parser/operation/parser
import ream/storage/schema/data_type

pub fn tokenize_test() {
  let assert Ok([
    lexer.Token(_, "city", lex.Name("city")),
    lexer.Token(_, "IN", lex.In),
    lexer.Token(_, "(", lex.LeftParens),
    lexer.Token(_, "\"London\"", lex.String("London")),
    lexer.Token(_, ",", lex.Coma),
    lexer.Token(_, "\"Madrid\"", lex.String("Madrid")),
    lexer.Token(_, ")", lex.RightParens),
    lexer.Token(_, "AND", lex.And),
    lexer.Token(_, "position", lex.Name("position")),
    lexer.Token(_, "!=", lex.NotEqual),
    lexer.Token(_, "\"commercial\"", lex.String("commercial")),
    lexer.Token(_, "AND", lex.And),
    lexer.Token(_, "email", lex.Name("email")),
    lexer.Token(_, "=~", lex.Regex),
    lexer.Token(_, "\"@mail.com$\"", lex.String("@mail.com$")),
  ]) =
    operation.tokenize(
      "city IN (\"London\",\"Madrid\") AND position != \"commercial\" AND email =~ \"@mail.com$\"",
    )
}

pub fn parse_test() {
  let assert Ok(tokens) =
    operation.tokenize(
      "city IN (\"London\",\"Madrid\") AND position != \"commercial\" AND email =~ \"@mail.com$\"",
    )
  let assert Ok(parser.And(
    parser.And(
      parser.In(
        parser.Field(data_type.String("city")),
        parser.Array([
          parser.Literal(data_type.String("London")),
          parser.Literal(data_type.String("Madrid")),
        ]),
      ),
      parser.NotEqual(
        parser.Field(data_type.String("position")),
        parser.Literal(data_type.String("commercial")),
      ),
    ),
    parser.Regex(
      parser.Field(data_type.String("email")),
      parser.Literal(data_type.String("@mail.com$")),
    ),
  )) = operation.parse(tokens)
}
