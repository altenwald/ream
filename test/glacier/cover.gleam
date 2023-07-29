import gleam/dynamic.{Dynamic}
import gleam/io
import gleam/list
import gleam/result
import gleam/string

pub fn cover_start() -> Nil {
  do_cover_start()
  find_files(matching: "**/*.{erl,gleam}", in: "src")
  |> list.map(gleam_to_erlang_module_name)
  |> list.map(dangerously_convert_string_to_atom(_, Utf8))
  |> list.each(recompile_for_coverage)
}

fn gleam_to_erlang_module_name(path: String) -> String {
  path
  |> string.replace(".gleam", "")
  |> string.replace(".erl", "")
  |> string.replace("/", "@")
}

fn recompile_for_coverage(module) -> Atom {
  case do_cover_recompile_module(module) {
    Ok(_) -> {
      module
    }
    Error(err) -> {
      io.println_error("cannot compile coverage: " <> err)
      module
    }
  }
}

@external(erlang, "cover_ffi", "start_coverage")
fn do_cover_start() -> Nil

@external(erlang, "cover_ffi", "stop_coverage")
fn do_cover_stop() -> Nil

@external(erlang, "cover_ffi", "compile_coverage")
fn do_cover_recompile_module(module module: Atom) -> Result(Nil, String)

@external(erlang, "cover_ffi", "find_files")
fn find_files(matching matching: String, in in: String) -> List(String)

type Atom

type Encoding {
  Utf8
}

pub fn cover_stop() -> Nil {
  do_cover_stop()
}

@external(erlang, "erlang", "binary_to_atom")
fn dangerously_convert_string_to_atom(a: String, b: Encoding) -> Atom

pub fn main(opts: List(GleeunitProgressOption)) -> Nil {
  let options = [Verbose, NoTty, Report(#(GleeunitProgress, opts))]

  do_cover_start()
  find_files(matching: "**/*.{erl,gleam}", in: "src")
  |> list.map(gleam_to_erlang_module_name)
  |> list.map(dangerously_convert_string_to_atom(_, Utf8))
  |> list.each(recompile_for_coverage)

  let result =
    find_files(matching: "**/*.{erl,gleam}", in: "test")
    |> list.map(gleam_to_erlang_module_name)
    |> list.map(dangerously_convert_string_to_atom(_, Utf8))
    |> run_eunit(options)
    |> dynamic.result(dynamic.dynamic, dynamic.dynamic)
    |> result.unwrap(Error(dynamic.from(Nil)))

  do_cover_stop()

  let code = case result {
    Ok(_) -> 0
    Error(_) -> 1
  }
  halt(code)
}

@external(erlang, "erlang", "halt")
fn halt(a: Int) -> Nil

pub type GleeunitProgressOption {
  Colored(Bool)
  Profile(Bool)
  Coverage(Bool)
}

type EunitOption {
  Verbose
  NoTty
  Report(#(ReportModuleName, List(GleeunitProgressOption)))
}

type ReportModuleName {
  GleeunitProgress
}

@external(erlang, "eunit", "test")
fn run_eunit(a: List(Atom), b: List(EunitOption)) -> Dynamic
