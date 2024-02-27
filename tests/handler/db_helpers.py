def assert_table_exists(conn, table_name):
    """Validates that a table exists in a connection's SQLite database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    assert cursor.fetchone(), f"Table '{table_name}' does not exist."


# TODO: Refactor this func it's a bit messy
def assert_table_schema(
    conn, table_name, expected_schema: list[tuple[str, str, bool, bool]]
):
    """
    Validates the schema of a given table against the expected schema.

    Parameters:
    - conn: SQLite connection object
    - table_name: Name of the table to validate
    - expected_schema: A list of tuples, where each tuple contains:
        (col_name, dtype, nullable, prime_key)

    Raises:
    - AssertionError: If the table does not exist or if any aspect of the schema does not match.
    """
    cursor = conn.cursor()

    # Retrieve table schema
    cursor.execute(f"PRAGMA table_info({table_name})")
    real_schema = cursor.fetchall()

    # Convert actual_schema to a more comparable format
    real_schema_dict = {
        row[1]: (row[2], bool(row[3] == 0), bool(row[5])) for row in real_schema
    }

    # Validate schema
    for colname, dtype, nullable, pkey in expected_schema:
        # Destructure actual characteristics of table columns
        (
            real_dtype,
            real_nullable,
            real_pkey,
        ) = real_schema_dict[colname]
        # raise LookupError(real_schema_dict)

        # Column-level characteristics assertions with messages
        assert (
            colname in real_schema_dict
        ), f"'{colname}' not found in table '{table_name}'."

        assert (
            real_dtype.lower() == dtype.lower()
        ), f"Wrong data type for column '{colname}', expected {dtype}, got {real_dtype}"

        assert (
            real_nullable == nullable
        ), f"Column '{colname}' should {'' if nullable else 'NOT'} be nullable."

        assert (
            real_pkey == pkey
        ), f"Column {colname} should {pkey and 'NOT'} be a primary key."
