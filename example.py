import sys

from sqlalchemy import create_engine

import pg_copy

if __name__ == "__main__":
    engine = create_engine(sys.argv[1])
    target_table = 'example_table'

    objs = [
        {
            'id': i,
            'description': f'record description {i}'
        } for i in range(100_000)
    ]

    pg_copy.insert_with_copy(engine, objs, target_table)
