import gleam/erlang/file
import gleam/erlang/process.{Subject}
import gleam/list
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import gleam/result.{try}
import ream/storage/file as fs
import ream/storage/file/read
import ream/storage/memtable.{MemTable}
import ream/storage/sstable
import ream/uuid

const min_bound = 0

const max_bound = 340_282_366_920_938_463_463_374_607_431_768_211_455

pub type MemTableRange {
  MemTableRange(lower: Int, upper: Int, memtable: Option(MemTable))
}

fn new_id() -> Int {
  uuid.to_int(uuid.new())
}

pub fn load(
  base_path: String,
  max_memtable_size: Int,
) -> #(Int, Map(Int, MemTableRange)) {
  let key_index_file = fs.join([base_path, "key", "index"])
  let assert Ok(kv) = fs.open(key_index_file, [fs.Read, fs.Write])
  let ranges = read_memtable_ranges(kv, base_path, max_memtable_size, map.new())
  let #(memtables_loaded, ranges) = case map.size(ranges) == 0 {
    True -> {
      let memtable = memtable.new(max_memtable_size)
      let ranges =
        [#(new_id(), MemTableRange(min_bound, max_bound, Some(memtable)))]
        |> map.from_list()
      #(1, ranges)
    }
    False -> #(0, ranges)
  }
  let assert Ok(_) = fs.close(kv)
  #(memtables_loaded, ranges)
}

pub fn flush(
  base_path: String,
  sstable_kind: sstable.Kind,
  ranges: Map(Int, MemTableRange),
) -> Result(Bool, file.Reason) {
  let assert Ok(kv_file) =
    fs.open(fs.join([base_path, "key", "index"]), [fs.Write])
  let memtable_ranges = map.to_list(ranges)
  case
    write_memtable_ranges(kv_file, sstable_kind, base_path, memtable_ranges)
  {
    Ok(_) -> fs.close(kv_file)
    Error(error) -> {
      use _ <- try(fs.close(kv_file))
      Error(error)
    }
  }
}

fn write_memtable_ranges(
  kv_file: Subject(fs.Message),
  sstable_kind: sstable.Kind,
  base_path: String,
  memtable_ranges: List(#(Int, MemTableRange)),
) -> Result(Nil, file.Reason) {
  case memtable_ranges {
    [#(id, MemTableRange(lower, upper, Some(memtable))), ..rest] -> {
      let assert Ok(True) =
        sstable.flush(memtable, sstable.path(base_path, sstable_kind, id))
      let assert Ok(_) =
        fs.write(kv_file, fs.Cur(0), <<lower:128, upper:128, id:128>>)
      write_memtable_ranges(kv_file, sstable_kind, base_path, rest)
    }
    [#(id, MemTableRange(lower, upper, None)), ..rest] -> {
      let assert Ok(_) =
        fs.write(kv_file, fs.Cur(0), <<lower:128, upper:128, id:128>>)
      write_memtable_ranges(kv_file, sstable_kind, base_path, rest)
    }
    [] -> Ok(Nil)
  }
}

fn read_memtable_ranges(
  kv: Subject(fs.Message),
  path: String,
  max_size: Int,
  acc: Map(Int, MemTableRange),
) -> Map(Int, MemTableRange) {
  case fs.read(kv, fs.Cur(0), 48) {
    read.Ok(<<lower:size(128), upper:size(128), id:size(128)>>) -> {
      let range = MemTableRange(lower, upper, None)
      read_memtable_ranges(kv, path, max_size, map.insert(acc, id, range))
    }
    read.Eof -> acc
    read.Error(_err) -> {
      let assert Ok(_) = fs.close(kv)
      panic as "unexpected error reading memtable ranges"
    }
  }
}

pub fn find(
  base_path: String,
  memtable_ranges: Map(Int, MemTableRange),
  memtables_loaded: Int,
  max_memtables_loaded: Int,
  sstable_kind: sstable.Kind,
  key_hash: Int,
  max_size: Int,
) -> #(Int, Map(Int, MemTableRange), Int) {
  let assert #(#(loaded, Some(range_id)), range_list) =
    memtable_ranges
    |> map.to_list()
    |> list.map_fold(
      #(memtables_loaded, None),
      fn(acc, entry) {
        let #(id, range) = entry
        let #(loaded, range_id) = acc
        case
          key_hash >= range.lower && key_hash <= range.upper,
          range.memtable
        {
          True, Some(_) -> #(#(loaded, Some(id)), #(id, range))
          True, None -> {
            let assert Ok(memtable) =
              sstable.load(sstable.path(base_path, sstable_kind, id), max_size)
            #(
              #(loaded + 1, Some(id)),
              #(id, MemTableRange(..range, memtable: Some(memtable))),
            )
          }
          False, Some(memtable) if loaded >= max_memtables_loaded -> {
            let assert Ok(_) =
              sstable.flush(memtable, sstable.path(base_path, sstable_kind, id))
            #(
              #(loaded - 1, range_id),
              #(id, MemTableRange(..range, memtable: None)),
            )
          }
          False, _ -> #(acc, #(id, range))
        }
      },
    )

  #(range_id, map.from_list(range_list), loaded)
}

pub fn split(
  ranges: Map(Int, MemTableRange),
  base_path: String,
  sstable_kind: sstable.Kind,
  key_hash: Int,
  range_id: Int,
  range: MemTableRange,
  memtable: MemTable,
) -> Map(Int, MemTableRange) {
  let #(memtable_low, memtable_high, pivot) = memtable.split(memtable, key_hash)
  let assert MemTableRange(lower, upper, _) = range
  let #(memtable_range_low, memtable_range_high) = case
    memtable_low.size >= memtable_high.size
  {
    False -> #(
      MemTableRange(lower, pivot - 1, Some(memtable_low)),
      MemTableRange(pivot, upper, None),
    )
    True -> #(
      MemTableRange(lower, pivot - 1, None),
      MemTableRange(pivot, upper, Some(memtable_high)),
    )
  }
  let memtable_high_id = new_id()
  let memtable_ranges =
    ranges
    |> map.insert(range_id, memtable_range_low)
    |> map.insert(memtable_high_id, memtable_range_high)

  let assert Ok(True) =
    sstable.flush(
      memtable_high,
      sstable.path(base_path, sstable_kind, memtable_high_id),
    )

  let assert Ok(True) =
    sstable.flush(memtable_low, sstable.path(base_path, sstable_kind, range_id))

  memtable_ranges
}
