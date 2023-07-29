import gleam/erlang/file
import ream/storage/schema
import ream/storage/schema/table.{Field, Table}
import ream/storage/file as fs

const base_path = "build/schema_test/"

pub fn create_test() {
  let path = fs.join([base_path, "create_test"])
  let _ = file.recursive_delete(path)

  let table =
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
  let max_memtable_size = 4096
  let max_memtables_loaded = 5
  let max_value_size = 4096

  let accounts =
    schema.create(
      table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )
  let assert Ok(Nil) = schema.close(accounts)

  let assert True =
    Ok(table) == table.load(fs.join([path, "schema/accounts/meta"]))
}
