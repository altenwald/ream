import gleam/option.{None, Some}
import nibble
import nibble/pratt
import ream/protocol/parser/operation/lexer
import ream/storage/schema/data_type.{DataType}

pub type Context {
  SubExpression
}

pub type Parsed {
  Field(DataType)
  Literal(DataType)
  Array(List(Parsed))
  Contains(Parsed, Parsed)
  In(Parsed, Parsed)
  Equal(Parsed, Parsed)
  NotEqual(Parsed, Parsed)
  LesserThan(Parsed, Parsed)
  GreaterThan(Parsed, Parsed)
  LesserOrEqualThan(Parsed, Parsed)
  GreaterOrEqualThan(Parsed, Parsed)
  Regex(Parsed, Parsed)
  Or(Parsed, Parsed)
  And(Parsed, Parsed)
  Not(Parsed)
  All
}

pub fn parse() {
  pratt.expression(
    one_of: [
      pratt.prefix(5, nibble.token(lexer.Not), Not),
      subset_parser,
      parens_parser,
      elements_parser,
    ],
    and_then: [
      pratt.infix_left(10, nibble.token(lexer.LesserThan), LesserThan),
      pratt.infix_left(
        10,
        nibble.token(lexer.LesserOrEqualThan),
        LesserOrEqualThan,
      ),
      pratt.infix_left(10, nibble.token(lexer.GreaterThan), GreaterThan),
      pratt.infix_left(
        10,
        nibble.token(lexer.GreaterOrEqualThan),
        GreaterOrEqualThan,
      ),
      pratt.infix_left(10, nibble.token(lexer.Regex), Regex),
      pratt.infix_left(10, nibble.token(lexer.In), In),
      pratt.infix_left(10, nibble.token(lexer.Contains), Contains),
      pratt.infix_left(9, nibble.token(lexer.Equal), Equal),
      pratt.infix_left(9, nibble.token(lexer.NotEqual), NotEqual),
      pratt.infix_left(5, nibble.token(lexer.And), And),
      pratt.infix_left(4, nibble.token(lexer.Or), Or),
    ],
    dropping: nibble.return(Nil),
  )
}

fn elements_parser(_) {
  use tok <- nibble.take_map("element token")

  case tok {
    lexer.Null -> Some(Literal(data_type.Null))
    lexer.Integer(n) -> Some(Literal(data_type.Integer(n)))
    lexer.Float(f) -> Some(Literal(data_type.Float(f)))
    lexer.String(s) -> Some(Literal(data_type.String(s)))
    // TODO bitstring?
    // TODO timestamp?
    lexer.True -> Some(Literal(data_type.Boolean(True)))
    lexer.False -> Some(Literal(data_type.Boolean(False)))
    lexer.Name(name) -> Some(Field(data_type.String(name)))
    _ -> None
  }
}

fn subset_parser(_) {
  use _ <- nibble.do(nibble.token(lexer.LeftParens))
  use elements <- nibble.do(nibble.sequence(
    nibble.lazy(parse),
    nibble.token(lexer.Coma),
  ))
  use _ <- nibble.do(nibble.token(lexer.RightParens))

  nibble.return(Array(elements))
}

fn parens_parser(_) {
  use _ <- nibble.do(nibble.token(lexer.LeftParens))
  use n <- nibble.do_in(SubExpression, nibble.lazy(parse))
  use _ <- nibble.do(nibble.token(lexer.RightParens))

  nibble.return(n)
}
