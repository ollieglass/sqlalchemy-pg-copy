import json
import io
import datetime as dt

# ------------------------------------------------------------------------------
# CSV pre-processing

def string_to_pg_csv_string(v):
    v = v.replace('\\', '\\\\')  # replace single \ with \\

    v = v.replace('\n', '\\n')
    v = v.replace('\r', '\\r')
    v = v.replace('"', '""')

    # can't store unicode null in Postgres so replace with empty string
    # https://stackoverflow.com/a/28816294/160406
    v = v.replace('\0', '')
    v = v.replace('\x00', '')
    v = v.replace(u'\u0000', '')

    return f'"{v}"'

def value_to_pg_csv_value(v):
    if v is None:
        return r'\N'

    elif isinstance(v, str):
        return string_to_pg_csv_string(v)

    elif isinstance(v, dt.datetime):
        return f'"{v.isoformat()}"'

    return str(v)

def list_of_dicts_to_pg_csv_lines(objs):
    columns = objs[0].keys()

    for o in objs:
        values = [ value_to_pg_csv_value(obj[c]) for c in columns ]
        line = ','.join(values) + '\n'
        yield line

# ------------------------------------------------------------------------------
# StringIO

def lines_to_stringio(lines):
    f = io.StringIO()

    for l in lines:
        f.write(l)
    f.seek(0)

    return f

# ------------------------------------------------------------------------------
# Insert

def insert_with_copy(engine, objs, target_table, target_schema=None):
    """Fast insert to a Postgres SQL table using COPY

    Handles duplicates by
    - inserting to a temp table
    - INSERT INTO from temp to target with ON CONFLICT DO NOTHING
    - dropping temp table

    Args:
        engine: SQLAclhemy engine object
        objs: list of dictionaries. Keys must be identical
        target_table: name of table to be uploaded to
        target_schema: optional
    """

    staging_table = f'staging_{target_table}'

    if target_schema:
        target_table = f'{target_schema}.{target_table}'

    column_names = ','.join(objs[0].keys())

    pg_csv_lines = list_of_dicts_to_pg_csv_lines(objs)
    f = lines_to_stringio(pg_csv_lines)

    # copy to staging table and de-dupe on insert, else COPY will crash on duplicates
    conn = engine.raw_connection()
    with conn.cursor() as cursor:

        cursor.execute(f'''
            CREATE TEMP TABLE {staging_table}
            ON COMMIT DROP
            AS
                SELECT *
                FROM {target_table}
                WITH NO DATA
        ''')

        cursor.copy_expert(f"""
            COPY {staging_table} ({column_names})
            FROM STDIN WITH CSV NULL '\\N'
        """, f)

        cursor.execute(f'''
            INSERT INTO {target_table} ({column_names})
            SELECT {column_names}
            FROM {staging_table}
            ON CONFLICT DO NOTHING
        ''')

        conn.commit()
        conn.close()
