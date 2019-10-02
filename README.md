# minoan-athenaeum

I wanted to build a columnar data store. So I did it's a command-line program
that loads table data into memory and executes simple SQL queries on that data.

It uses an easy-to-use [JSON structure](#json-formatted-sql).

## Toy Examples

- Make sure you have Python 2.7+ or 3.2+ installed (check: `python --version`).
- Example table data: "countries.table.json" and "cities.table.json"
- Example queries: the ".sql" files.
- Expected output for each query: the ".out" files.  (The order of the rows doesn't matter.)

To test the "example-1.sql" query:

```
$ ./sql-to-json example-1.sql > example-1.json
$ YOUR-PROGRAM example-1.json
```

In the Bash shell, you can do this without creating an intermediate ".json" file:

```
$ YOUR-PROGRAM <(./sql-to-json example-1.sql)
```

## File Formats

### Table JSON

Each file is a JSON array of rows.  The first element of the array is a list of column definitions.  Each column definition is an array where the first element is the column name and the second element is the column type.  The column type can be "str" or "int".

The rest of the elements are the row values.  They will be strings or integers.

See cities.table.json for an example.

### SQL Syntax

NOTE: You don't have to write a parser for this syntax.  Use the included Python program `sql-to-json` to convert SQL to a JSON-formatted equivalent.

NOTE: This is heavily based on standard SQL but isn't exactly compatible.

```
Query =
    "SELECT" Selector ("," Selector)*
    "FROM" TableRef ("," TableRef)*
    ( "WHERE" Comparison ("AND" Comparison)* )?

Selector = ColumnRef ( "AS" <identifier> )?

TableRef = <identifier> ( "AS" <identifier> )?

Comparison = Term ( "=" | "!=" | ">" | ">=" | "<" | "<=" ) Term

Term = ColumnRef | <string-literal> | <integer-literal>

ColumnRef = <identifier> ( "." <identifier> )?
```

Comments start with "--" and go to the end of the line.

### JSON-formatted SQL

```
Query = {
    select: Array<Selector> // non-empty array
    from: Array<TableRef> // non-empty array
    where: Array<Comparison>
}

Selector = {
    source: {column: ColumnRef}
    as: string | null  // Set when there's an "AS" clause
}

TableRef = {
    source: {file: string}
    as: string | null  // Set when there's an "AS" clause
}

Comparison = {
    op: "=" | "!=" | ">" | ">=" | "<" | "<="
    left: Term
    right: Term
}

Term = {column: ColumnRef} | {lit_int: int} | {lit_str: string}

ColumnRef = {
    name: string
    table: string | null  // Set for fully-qualified references ("table1.column2")
}
```
