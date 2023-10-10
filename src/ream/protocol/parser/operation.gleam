import gleam/bit_string
import gleam/list
import gleam/result.{try}
import nibble
import ream/protocol/parser/operation/lexer
import ream/protocol/parser/operation/parser
import ream/storage/schema
import ream/storage/schema/table
import ream/storage/schema/data_type

pub fn tokenize(text) {
  lexer.tokenize(text)
}

pub fn parse(tokens) {
  nibble.run(tokens, parser.parse())
}

pub fn process(text) {
  let assert Ok(text) = bit_string.to_string(text)
  case tokenize(text) {
    Ok(tokens) ->
      case parse(tokens) {
        Ok(parsed) -> Ok(parsed)
        Error(_parsed_error) -> {
          // TODO do something with parsed_error?
          Error("cannot parse")
        }
      }
    Error(_error) -> {
      // TODO do something with parsed_error?
      Error("cannot tokenize text")
    }
  }
}

pub fn to_schema(parsed, table) {
  case parsed {
    parser.All -> Ok(schema.All)
    parser.And(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.And(left, right))
    }
    parser.Array(elements) ->
      Ok(schema.Array(list.map(elements, to_data_schema(_, table))))
    parser.Contains(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.Contains(left, right))
    }
    parser.Equal(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.Equal(left, right))
    }
    parser.Field(_) -> Ok(schema.Data(to_data_schema(parsed, table)))
    parser.GreaterOrEqualThan(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.GreaterOrEqualThan(left, right))
    }
    parser.GreaterThan(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.GreaterThan(left, right))
    }
    parser.In(left, right) -> {
      let assert parser.Array(elements) = right
      use left <- try(to_schema(left, table))
      Ok(schema.In(left, list.map(elements, to_data_schema(_, table))))
    }
    parser.LesserOrEqualThan(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.LesserOrEqualThan(left, right))
    }
    parser.LesserThan(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.LesserThan(left, right))
    }
    parser.Literal(data) -> Ok(schema.Data(schema.Literal(data)))
    parser.Not(element) -> {
      use element <- try(to_schema(element, table))
      Ok(schema.Not(element))
    }
    parser.NotEqual(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.NotEqual(left, right))
    }
    parser.Or(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.Or(left, right))
    }
    parser.Regex(left, right) -> {
      use left <- try(to_schema(left, table))
      use right <- try(to_schema(right, table))
      Ok(schema.Regex(left, right))
    }
  }
}

fn to_data_schema(parsed, table) {
  case parsed {
    parser.Field(data_type.String(name)) -> {
      let assert Ok(id) = table.field_idx(table, name)
      schema.Field(id)
    }
    parser.Literal(data) -> schema.Literal(data)
    _ -> panic
  }
}
