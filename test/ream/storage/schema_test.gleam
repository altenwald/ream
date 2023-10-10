import gleam/erlang/file
import gleam/list
import ream/storage/schema
import ream/storage/schema/data_type as dt
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

pub fn find_test() {
  let path = fs.join([base_path, "find_test"])
  let _ = file.recursive_delete(path)

  let table =
    Table(
      name: "user",
      fields: [
        Field(1, "id", table.Integer, False),
        Field(2, "name", table.String(table.Unlimited), False),
        Field(3, "age", table.Integer, False),
      ],
      primary_key: [1],
      indexes: [],
    )

  let assert Ok(users) =
    schema.create(
      table,
      max_memtable_size,
      max_memtables_loaded,
      max_value_size,
      path,
    )

  let data = [
    [
      #("id", dt.Integer(1)),
      #("name", dt.String("manuel")),
      #("age", dt.Integer(43)),
    ],
    [
      #("id", dt.Integer(2)),
      #("name", dt.String("antonio")),
      #("age", dt.Integer(33)),
    ],
    [
      #("id", dt.Integer(3)),
      #("name", dt.String("ana")),
      #("age", dt.Integer(27)),
    ],
    [
      #("id", dt.Integer(4)),
      #("name", dt.String("matusalen")),
      #("age", dt.Integer(90)),
    ],
    [
      #("id", dt.Integer(5)),
      #("name", dt.String("jesus")),
      #("age", dt.Integer(3)),
    ],
  ]
  let users =
    list.fold(
      data,
      users,
      fn(users, user) {
        let assert Ok(users) = schema.insert(users, user)
        users
      },
    )

  let assert #(
    Ok([
      [dt.Integer(1), dt.String("manuel"), dt.Integer(43)],
      [dt.Integer(2), dt.String("antonio"), dt.Integer(33)],
      [dt.Integer(3), dt.String("ana"), dt.Integer(27)],
      [dt.Integer(4), dt.String("matusalen"), dt.Integer(90)],
    ]),
    users,
  ) =
    schema.find(
      users,
      schema.GreaterOrEqualThan(
        schema.Data(schema.Field(3)),
        schema.Data(schema.Literal(dt.Integer(18))),
      ),
    )

  let assert #(Ok([[dt.Integer(5), dt.String("jesus"), dt.Integer(3)]]), users) =
    schema.find(
      users,
      schema.LesserThan(
        schema.Data(schema.Field(3)),
        schema.Data(schema.Literal(dt.Integer(18))),
      ),
    )

  let assert #(
    Ok([[dt.Integer(1), dt.String("manuel"), dt.Integer(43)]]),
    users,
  ) =
    schema.find(
      users,
      schema.Equal(
        schema.Data(schema.Field(2)),
        schema.Data(schema.Literal(dt.String("manuel"))),
      ),
    )

  let assert #(
    Ok([[dt.Integer(1), dt.String("manuel"), dt.Integer(43)]]),
    users,
  ) =
    schema.find(
      users,
      schema.Contains(
        schema.Data(schema.Field(2)),
        schema.Data(schema.Literal(dt.String("man"))),
      ),
    )

  let assert #(Ok([]), users) =
    schema.find(
      users,
      schema.Equal(
        schema.Data(schema.Field(2)),
        schema.Data(schema.Literal(dt.String("manu"))),
      ),
    )

  let assert Ok(Nil) = schema.close(users)
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
        #("id", dt.Integer(1)),
        #("name", dt.String("Bank")),
        #("debit", dt.Decimal(10_000, 2)),
        #("balance", dt.Decimal(10_000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Ok(accounts) =
    schema.insert(
      accounts,
      [
        #("id", dt.Integer(1)),
        #("name", dt.String("Active")),
        #("balance", dt.Decimal(100, 0)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_000)),
      ],
    )

  let assert #(
    Ok([
      [
        dt.Integer(1),
        dt.String("Active"),
        dt.Null,
        dt.Null,
        dt.Decimal(100, 0),
        dt.Timestamp(1_690_785_424_366_000),
      ],
    ]),
    accounts,
  ) =
    schema.find(
      accounts,
      schema.Equal(
        schema.Data(schema.Field(1)),
        schema.Data(schema.Literal(dt.Integer(1))),
      ),
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
        #("name", dt.String("1984")),
        #("value", dt.Decimal(1000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_972)),
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
        #("type", dt.String("Books")),
        #("class", dt.Null),
        #("name", dt.String("1984")),
        #("value", dt.Decimal(1000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_972)),
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
        #("type", dt.Null),
        #("class", dt.String("Fantasy")),
        #("name", dt.String("1984")),
        #("value", dt.Decimal(1000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_972)),
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
        #("id", dt.Integer(1)),
        #("name", dt.String("Bank")),
        #("debit", dt.Decimal(10_000, 2)),
        #("balance", dt.Decimal(10_000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_972)),
      ],
    )

  let assert Ok(accounts) =
    schema.insert(
      accounts,
      [
        #("id", dt.Integer(2)),
        #("name", dt.String("Cash")),
        #("debit", dt.Decimal(5000, 2)),
        #("balance", dt.Decimal(5000, 2)),
        #("inserted_at", dt.Timestamp(1_690_785_424_366_000)),
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

  let #(
    Ok([
      [
        dt.Integer(1),
        dt.String("Bank"),
        dt.Null,
        dt.Decimal(10_000, 2),
        dt.Decimal(10_000, 2),
        dt.Timestamp(1_690_785_424_366_972),
      ],
    ]),
    saved_accounts,
  ) =
    schema.find(
      saved_accounts,
      schema.Equal(
        schema.Data(schema.Field(1)),
        schema.Data(schema.Literal(dt.Integer(1))),
      ),
    )

  let #(
    Ok([
      [
        dt.Integer(1),
        dt.String("Bank"),
        dt.Null,
        dt.Decimal(10_000, 2),
        dt.Decimal(10_000, 2),
        dt.Timestamp(1_690_785_424_366_972),
      ],
      [
        dt.Integer(2),
        dt.String("Cash"),
        dt.Null,
        dt.Decimal(5000, 2),
        dt.Decimal(5000, 2),
        dt.Timestamp(1_690_785_424_366_000),
      ],
    ]),
    saved_accounts,
  ) = schema.find(saved_accounts, schema.All)

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
