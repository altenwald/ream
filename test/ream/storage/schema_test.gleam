import gleam/erlang/file
import ream/storage/schema
import ream/storage/schema/data_type
import ream/storage/schema/table.{Field, Table}
import ream/storage/file as fs

const base_path = "build/schema_test/"

const max_memtable_size = 4096

const max_memtables_loaded = 5

const max_value_size = 4096

pub fn create_test() {
  let path = fs.join([base_path, "create_test"])
  let _ = file.recursive_delete(path)

  let accounts_table =
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

  let users_table =
    Table(
      name: "users",
      fields: [
        Field(1, "id", table.Integer, False),
        Field(2, "name", table.String(table.Size(50)), False),
        Field(3, "password", table.String(table.Size(50)), False),
        Field(4, "role", table.String(table.Size(50)), False),
        Field(5, "inserted_at", table.Timestamp, False),
      ],
      primary_key: [1],
      indexes: [table.Unique([2])],
    )

  let assert Ok(accounts) =
    schema.create(
      accounts_table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )

  let assert Ok(users) =
    schema.create(
      users_table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )

  let assert Ok(Nil) = schema.close(accounts)
  let assert Ok(Nil) = schema.close(users)

  let assert True =
    Ok(accounts_table) == table.load(fs.join([path, "schema/accounts/meta"]))

  let assert True =
    Ok(users_table) == table.load(fs.join([path, "schema/users/meta"]))
}

pub fn wrong_pk_create_test() {
  let path = fs.join([base_path, "wrong_pk_create_test"])

  let table =
    Table(
      name: "wrong",
      fields: [Field(1, "id", table.Integer, True)],
      primary_key: [1],
      indexes: [],
    )

  let assert Error(table.PrimaryKeyCannotBeNull(Field(
    1,
    "id",
    table.Integer,
    True,
  ))) =
    schema.create(
      table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )
}

pub fn wrong_ref_create_test() {
  let path = fs.join([base_path, "wrong_pk_create_test"])

  let table =
    Table(
      name: "ref",
      fields: [Field(1, "id", table.Integer, False)],
      primary_key: [2],
      indexes: [],
    )

  let assert Error(table.PrimaryKeyRefInvalid(2)) =
    schema.create(
      table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )
}

pub fn insert_test() {
  let path = fs.join([base_path, "insert_test"])
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

  let assert Ok(accounts) =
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

  let assert Ok(accounts) =
    schema.insert(
      accounts,
      [
        #("id", data_type.Integer(1)),
        #("name", data_type.String("Active")),
        #("balance", data_type.Decimal(100, 0)),
        #("inserted_at", data_type.Timestamp(1_690_785_424_366_000)),
      ],
    )

  let assert #(
    Ok([
      [
        data_type.Integer(1),
        data_type.String("Active"),
        data_type.Null,
        data_type.Null,
        data_type.Decimal(100, 0),
        data_type.Timestamp(1_690_785_424_366_000),
      ],
    ]),
    accounts,
  ) =
    schema.find(
      accounts,
      schema.Equal(schema.Field(1), schema.Literal(data_type.Integer(1))),
    )

  let assert Ok(Nil) = schema.close(accounts)
}

pub fn invalid_primary_key_test() {
  let path = fs.join([base_path, "invalid_pk_test"])
  let _ = file.recursive_delete(path)

  let table =
    Table(
      name: "accounts",
      fields: [
        Field(1, "type", table.String(table.Size(30)), False),
        Field(2, "class", table.String(table.Size(30)), False),
        Field(3, "name", table.String(table.Size(150)), False),
        Field(4, "value", table.Decimal, False),
        Field(5, "inserted_at", table.Timestamp, False),
      ],
      primary_key: [1, 2],
      indexes: [],
    )

  let assert Ok(accounts) =
    schema.create(
      table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )

  let assert Error(table.FieldCannotBeNull(table.Field(
    id: 1,
    name: "type",
    field_type: table.String(table.Size(30)),
    nilable: False,
  ))) =
    schema.insert(
      accounts,
      [
        #("name", data_type.String("1984")),
        #("value", data_type.Decimal(1000, 2)),
        #("inserted_at", data_type.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Error(table.FieldCannotBeNull(table.Field(
    id: 2,
    name: "class",
    field_type: table.String(table.Size(30)),
    nilable: False,
  ))) =
    schema.insert(
      accounts,
      [
        #("type", data_type.String("Books")),
        #("class", data_type.Null),
        #("name", data_type.String("1984")),
        #("value", data_type.Decimal(1000, 2)),
        #("inserted_at", data_type.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Error(table.FieldCannotBeNull(table.Field(
    id: 1,
    name: "type",
    field_type: table.String(table.Size(30)),
    nilable: False,
  ))) =
    schema.insert(
      accounts,
      [
        #("type", data_type.Null),
        #("class", data_type.String("Fantasy")),
        #("name", data_type.String("1984")),
        #("value", data_type.Decimal(1000, 2)),
        #("inserted_at", data_type.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Ok(Nil) = schema.close(accounts)
}

pub fn create_insert_close_open_find_and_close_test() {
  let path = fs.join([base_path, "full_test"])
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

  let assert Ok(accounts) =
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
