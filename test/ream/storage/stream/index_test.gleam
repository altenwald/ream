import gleam/erlang/file
import ream/storage/stream/index.{Index}

pub fn open_and_close_ok_test() {
  let path = "build/index_test/stream/entries"
  let _ = file.recursive_delete(path)

  let assert Ok(entries) = index.open(path)
  let assert Ok(Nil) = index.close(entries)
}

pub fn handle_two_different_files_test() {
  let path = "build/index_test/stream/multiple"
  let _ = file.recursive_delete(path)

  let <<file_id1:128>> = <<0:128>>
  let <<file_id2:128>> = <<1:128>>

  let assert Ok(multiple) = index.open(path)
  let assert #(event1, multiple) = index.add(multiple, 100, file_id1)
  let assert 0 = event1.offset
  let assert True = file_id1 == event1.file_id
  let assert #(event2, multiple) = index.add(multiple, 100, file_id2)
  let assert 0 = event2.offset
  let assert True = file_id2 == event2.file_id
  let assert #(event3, multiple) = index.add(multiple, 100, file_id1)
  let assert 103 = event3.offset
  let assert True = file_id1 == event3.file_id
  let assert #(event4, multiple) = index.add(multiple, 100, file_id2)
  let assert 103 = event4.offset
  let assert True = file_id2 == event4.file_id
  let assert Ok(Nil) = index.close(multiple)
}

pub fn add_and_retrieve_random_events_test() {
  let path = "build/index_test/stream/random"
  let _ = file.recursive_delete(path)

  let <<file_id:128>> = <<0:128>>

  let assert Ok(random) = index.open(path)
  let assert #(_event, random) = index.add(random, 10, file_id)
  let assert #(_event, random) = index.add(random, 20, file_id)
  let assert #(_event, random) = index.add(random, 30, file_id)
  let assert #(_event, random) = index.add(random, 25, file_id)

  let assert 100 = random.size
  let assert 4 = index.count(random)

  let assert Ok(Index(13, 23, 0)) = index.get(random, 1)
  let assert Ok(Index(69, 28, 0)) = index.get(random, 3)
  let assert Ok(Index(0, 13, 0)) = index.get(random, 0)
  let assert Ok(Index(36, 33, 0)) = index.get(random, 2)

  let assert Ok(Nil) = index.close(random)
}
