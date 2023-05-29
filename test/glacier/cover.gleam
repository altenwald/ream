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

external fn do_cover_start() -> Nil =
  "cover_ffi" "start_coverage"

external fn do_cover_stop() -> Nil =
  "cover_ffi" "stop_coverage"

external fn do_cover_recompile_module(module: Atom) -> Result(Nil, String) =
  "cover_ffi" "compile_coverage"

external fn find_files(matching: String, in: String) -> List(String) =
  "cover_ffi" "find_files"

external type Atom

type Encoding {
  Utf8
}

pub fn cover_stop() -> Nil {
  do_cover_stop()
}

external fn dangerously_convert_string_to_atom(String, Encoding) -> Atom =
  "erlang" "binary_to_atom"

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

external fn halt(Int) -> Nil =
  "erlang" "halt"

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

external fn run_eunit(List(Atom), List(EunitOption)) -> Dynamic =
  "eunit" "test"
