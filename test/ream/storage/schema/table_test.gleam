import gleam/erlang/file
import ream/storage/schema/table.{Field, Table}
import ream/storage/file as fs

const base_path = "build/schema_table_test/"

pub fn table_to_bitstring_test() {
  let accounts =
    Table(
      name: "accounts",
      fields: [
        Field(1, "id", table.Integer, False),
        Field(2, "name", table.String(table.Size(150)), False),
        Field(3, "credit", table.Decimal, True),
        Field(4, "debit", table.Decimal, True),
        Field(5, "balance", table.Decimal, False),
        Field(6, "inserted_at", table.Timestamp, False),
      ],
      primary_key: [1],
      indexes: [table.Unique([2]), table.Index([6, 2])],
    )
  let assert <<
    8:8,
    "accounts":utf8,
    // num_fields
    6:16,
    // field 1
    2:8,
    "id":utf8,
    0:8,
    0:8,
    0:8,
    // field 2
    4:8,
    "name":utf8,
    3:8,
    150:8,
    0:8,
    // field 3
    6:8,
    "credit":utf8,
    2:8,
    0:8,
    1:8,
    // field 4
    5:8,
    "debit":utf8,
    2:8,
    0:8,
    1:8,
    // field 5
    7:8,
    "balance":utf8,
    2:8,
    0:8,
    0:8,
    // field 6
    11:8,
    "inserted_at":utf8,
    5:8,
    0:8,
    0:8,
    // primary keys num
    1:16,
    // primary key 1
    1:16,
    // indexes num
    2:16,
    // index 1 type
    0:8,
    // index 1 num and element
    1:16,
    2:16,
    // index 2 type
    1:8,
    // index 2 num
    2:16,
    // index 2 element 1
    6:16,
    // index 2 element 2
    2:16,
  >> = table.to_bitstring(accounts)
}

pub fn table_from_bitstring_test() {
  let accounts_data = <<
    8, "accounts":utf8, 6:16, 2, "id":utf8, 0:8, 0:8, 0, 4, "name":utf8, 3:8,
    150:8, 0, 6, "credit":utf8, 2:8, 0:8, 1, 5, "debit":utf8, 2:8, 0:8, 1, 7,
    "balance":utf8, 2:8, 0:8, 0, 11, "inserted_at":utf8, 5:8, 0:8, 0:8,
    // primary keys
    1:16, 1:16,
    // indexes
    2:16, 0:8, 1:16, 2:16, 1:8, 2:16, 6:16, 2:16,
  >>
  let assert Table(
    name: "accounts",
    fields: [
      Field(1, "id", table.Integer, False),
      Field(2, "name", table.String(table.Size(150)), False),
      Field(3, "credit", table.Decimal, True),
      Field(4, "debit", table.Decimal, True),
      Field(5, "balance", table.Decimal, False),
      Field(6, "inserted_at", table.Timestamp, False),
    ],
    primary_key: [1],
    indexes: [table.Unique([2]), table.Index([6, 2])],
  ) = table.from_bitstring(accounts_data)
}

pub fn flush_test() {
  let path = fs.join([base_path, "flush_test", "schema"])
  let _ = file.recursive_delete(path)

  let path = fs.join([path, "accounts.schema"])

  let accounts =
    Table(
      name: "accounts",
      fields: [
        Field(1, "id", table.Integer, False),
        Field(2, "name", table.String(table.Size(150)), False),
        Field(3, "credit", table.Decimal, True),
        Field(4, "debit", table.Decimal, True),
        Field(5, "balance", table.Decimal, False),
        Field(6, "inserted_at", table.Timestamp, False),
      ],
      primary_key: [1],
      indexes: [],
    )

  let assert Ok(Nil) = table.flush(accounts, path)
  let assert Ok(<<
    8,
    "accounts":utf8,
    6:16,
    2,
    "id":utf8,
    0:8,
    0:8,
    0,
    4,
    "name":utf8,
    3:8,
    150:8,
    0,
    6,
    "credit":utf8,
    2:8,
    0:8,
    1,
    5,
    "debit":utf8,
    2:8,
    0:8,
    1,
    7,
    "balance":utf8,
    2:8,
    0:8,
    0,
    11,
    "inserted_at":utf8,
    5:8,
    0:8,
    0,
    1:16,
    1:16,
    0:16,
  >>) = file.read_bits(path)
}

pub fn load_test() {
  let path = fs.join([base_path, "load_test", "schema"])
  let _ = file.recursive_delete(path)

  let path = fs.join([path, "accounts.schema"])

  let accounts_data = <<
    8, "accounts":utf8, 6:16, 2, "id":utf8, 0:8, 0:8, 0, 4, "name":utf8, 3:8,
    150:8, 0, 6, "credit":utf8, 2:8, 0:8, 1, 5, "debit":utf8, 2:8, 0:8, 1, 7,
    "balance":utf8, 2:8, 0:8, 0, 11, "inserted_at":utf8, 5:8, 0:8, 0:8, 1:16,
    1:16, 0:16,
  >>
  let assert Ok(True) = fs.recursive_make_directory(fs.dirname(path))
  let assert Ok(Nil) = file.write_bits(accounts_data, path)
  let assert Ok(Table(
    name: "accounts",
    fields: [
      Field(1, "id", table.Integer, False),
      Field(2, "name", table.String(table.Size(150)), False),
      Field(3, "credit", table.Decimal, True),
      Field(4, "debit", table.Decimal, True),
      Field(5, "balance", table.Decimal, False),
      Field(6, "inserted_at", table.Timestamp, False),
    ],
    primary_key: [1],
    indexes: [],
  )) = table.load(path)
}
