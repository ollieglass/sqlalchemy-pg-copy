# sqlalchemy-pg-copy

Helper function to COPY a list of dicts to Postgres

## Motivation

- Postgres COPY is the fastest way to insert data
- SQLAlchemy has no function to perform a Postgres COPY
- COPY requires data to be in a specific CSV format
- Python's CSV library can't transform data to that format.

This helper function:
- takes a SQLAlchemy engine, list of dictionaries to insert, and table name
- transforms the list of dictionaries to Postgres COPY CSV format
- ignores duplicate records (by inserting via a temporary staging table)

## Example

```python
import pg_copy

objs = [
    {
        'id': i,
        'description': f'record description {i}'
    } for i in range(100_000)
]

pg_copy.insert_with_copy(engine, objs, target_table)
```

See [example.py](example.py)

## References

> Use COPY to load all the rows in one command, instead of using a series of INSERT commands. The COPY command is optimized for loading large numbers of rows; it is less flexible than INSERT, but incurs significantly less overhead for large data loads. Since COPY is a single command, there is no need to disable autocommit if you use this method to populate a table.

Postgres docs ([Populating a Database - Chapter 14. Performance Tips](https://www.postgresql.org/docs/current/populate.html#POPULATE-COPY-FROM))

Stack Overflow - [How does COPY work and why is it so much faster than INSERT?](https://stackoverflow.com/questions/46715354/how-does-copy-work-and-why-is-it-so-much-faster-than-insert)

SQLAlchemy docs, [Bulk Operations](https://docs.sqlalchemy.org/en/13/orm/persistence_techniques.html#bulk-operations)

Example code and benchmarks for all methods of inserting data into Postgres. COPY is ~2-200x faster than any other method [Fastest Way to Load Data Into PostgreSQL Using Python](https://hakibenita.com/fast-load-data-python-postgresql)