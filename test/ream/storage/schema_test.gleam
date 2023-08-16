import gleam/erlang/file
import ream/storage/schema
import ream/storage/schema/data_type
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

  let assert Ok(accounts) =
    schema.insert(
      accounts,
      [
        #("id", data_type.Integer(1)),
        #("name", data_type.String("Bank")),
        #("debit", data_type.Decimal(10_000, 2)),
        #("balance", data_type.Decimal(10_000, 2)),
        #("inserted_at", data_type.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Ok(Nil) = schema.close(accounts)

  let assert True =
    Ok(table) == table.load(fs.join([path, "schema/accounts/meta"]))

  let saved_accounts =
    schema.open(
      "accounts",
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )

  let #(Ok([result]), saved_accounts) =
    schema.find(
      saved_accounts,
      schema.Equal(schema.Field(1), schema.Literal(data_type.Integer(1))),
    )

  let assert [
    data_type.Integer(1),
    data_type.String("Bank"),
    data_type.Null,
    data_type.Decimal(10_000, 2),
    data_type.Decimal(10_000, 2),
    data_type.Timestamp(1_690_785_424_366_972),
  ] = result

  let assert Ok(Nil) = schema.close(saved_accounts)

  let assert True = saved_accounts.base_path == accounts.base_path
  let assert True = saved_accounts.table == accounts.table
  let assert True = saved_accounts.memtable_ranges == accounts.memtable_ranges
  let assert True =
    saved_accounts.value_index.active_value_file == accounts.value_index.active_value_file
  let assert True = saved_accounts.memtables_loaded == accounts.memtables_loaded
  let assert True =
    saved_accounts.max_memtables_loaded == accounts.max_memtables_loaded
  let assert True =
    saved_accounts.max_memtable_size == accounts.max_memtable_size
  let assert True = saved_accounts.max_value_size == accounts.max_value_size
}
