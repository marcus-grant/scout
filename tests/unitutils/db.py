def assert_table_schema(conn, table_name, expected_schema):
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

    # Check if table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    assert cursor.fetchone(), f"Table '{table_name}' does not exist."

    # Retrieve table schema
    cursor.execute(f"PRAGMA table_info({table_name})")
    actual_schema = cursor.fetchall()

    # Convert actual_schema to a more comparable format
    actual_schema_dict = {
        row[1]: (row[2], bool(row[3] == 0), bool(row[5])) for row in actual_schema
    }

    # Validate schema
    for column_name, data_type, is_nullable, is_primary_key in expected_schema:
        assert (
            column_name in actual_schema_dict
        ), f"Column '{column_name}' not found in table '{table_name}'."
        (
            actual_data_type,
            actual_is_nullable,
            actual_is_primary_key,
        ) = actual_schema_dict[column_name]
        assert (
            actual_data_type.lower() == data_type.lower()
        ), f"Data type mismatch for column '{column_name}': expected {data_type}, got {actual_data_type}."
        assert (
            actual_is_nullable == is_nullable
        ), f"Nullability mismatch for column '{column_name}': expected {is_nullable}, got {actual_is_nullable}."
        assert (
            actual_is_primary_key == is_primary_key
        ), f"Primary key mismatch for column '{column_name}': expected {is_primary_key}, got {actual_is_primary_key}."
