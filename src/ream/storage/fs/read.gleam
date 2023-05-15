import gleam/erlang/file

pub type Result {
  Ok(data: String)
  Eof
  Error(reason: file.Reason)
}
