import gleam/erlang/file
import gleam/int
import gleam/list
import ream/storage/kv

const base_path = "build/kv_test/kv/"

pub fn kv_test() {
  let path = base_path <> "basic"
  let _ = file.recursive_delete(path)

  let kv =
    kv.open("build/kv_test", "basic", 5, 1024, 1024)
    |> kv.set("hello", <<"hola":utf8>>)
    |> kv.set("world", <<"mundo":utf8>>)

  let assert #(Ok(<<"hola":utf8>>), kv) = kv.get(kv, "hello")
  let assert #(Error(Nil), kv) = kv.get(kv, "not-found")
  let assert #(Ok(<<"mundo":utf8>>), kv) = kv.get(kv, "world")

  let assert Ok(Nil) = kv.close(kv)
}

pub fn two_memtables_test() {
  let path = base_path <> "two_memtables"
  let _ = file.recursive_delete(path)

  let kv =
    kv.open("build/kv_test", "two_memtables", 1, 100, 1024)
    |> kv.set("key1", <<"value1":utf8>>)
    |> kv.set("key2", <<"value2":utf8>>)
    |> kv.set("key3", <<"value3":utf8>>)

  let assert #(Ok(<<"value1":utf8>>), kv) = kv.get(kv, "key1")
  let assert #(Ok(<<"value2":utf8>>), kv) = kv.get(kv, "key2")
  let assert #(Ok(<<"value3":utf8>>), kv) = kv.get(kv, "key3")

  let kvinfo = kv.info(kv)
  let assert 1 = kvinfo.memtables_loaded
  let assert 2 = kvinfo.memtables_total

  let assert Ok(Nil) = kv.close(kv)
}

pub fn two_value_files_test() {
  let path = base_path <> "two_value_files"
  let _ = file.recursive_delete(path)

  let kv =
    kv.open("build/kv_test", "two_value_files", 2, 1024, 20)
    |> kv.set("key1", <<"value":utf8>>)
    |> kv.set("key2", <<"value":utf8>>)
    |> kv.set("key3", <<"value":utf8>>)

  let assert #(Ok(<<"value":utf8>>), kv) = kv.get(kv, "key1")
  let assert #(Ok(<<"value":utf8>>), kv) = kv.get(kv, "key2")
  let assert #(Ok(<<"value":utf8>>), kv) = kv.get(kv, "key3")

  let kvinfo = kv.info(kv)
  let assert 2 = kvinfo.values
  let assert 30 = kvinfo.values_size_bytes

  let assert Ok(Nil) = kv.close(kv)
}

pub fn benchmark_set_test() {
  let path = base_path <> "benchmark_set"
  let _ = file.recursive_delete(path)

  let kv = kv.open("build/kv_test", "benchmark_set", 15, 4096, 40_960)
  let assert #(time, Ok(kv)) =
    tc(fn() {
      list.range(0, 999)
      |> list.fold(
        Ok(kv),
        fn(ok_kv, i) {
          let assert Ok(kv) = ok_kv
          Ok(kv.set(kv, "key-" <> int.to_string(i), <<i:256>>))
        },
      )
    })
  let assert True = time > 0 && time < 500_000
  let assert Ok(Nil) = kv.close(kv)
}

external fn tc(f: fn() -> Result(kv.KV, Nil)) -> #(Int, Result(kv.KV, Nil)) =
  "timer" "tc"
