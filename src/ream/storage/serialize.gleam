import gleam/dynamic.{Dynamic}

pub external fn encode(value: Dynamic) -> BitString =
  "erlang" "term_to_binary"

pub external fn decode(BitString) -> Dynamic =
  "erlang" "binary_to_term"
