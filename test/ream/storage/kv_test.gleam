import gleam/erlang/file
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
    kv.open("build/kv_test", "two_memtables", 2, 50, 1024)
    |> kv.set("key1", <<"value1":utf8>>)
    |> kv.set("key2", <<"value2":utf8>>)

  let assert #(Ok(<<"value1":utf8>>), kv) = kv.get(kv, "key1")
  let assert #(Ok(<<"value2":utf8>>), kv) = kv.get(kv, "key2")

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
