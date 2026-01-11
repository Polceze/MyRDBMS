import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing COUNT(*) fix...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

test_queries = [
    ("SELECT COUNT(*) FROM students", "Basic COUNT(*)"),
    ("SELECT COUNT(*) as total_students FROM students", "COUNT(*) with alias"),
    ("SELECT COUNT(student_id) FROM students", "COUNT(column)"),
    ("SELECT COUNT(email) FROM students", "COUNT(column) - counts non-null"),
    ("SELECT * FROM students LIMIT 2", "Regular SELECT"),
    ("SELECT first_name, last_name FROM students WHERE enrollment_year = 2022", "SELECT with WHERE"),
]

for sql, description in test_queries:
    print(f"\n{'='*60}")
    print(f"{description}:")
    print(f"SQL: {sql}")
    try:
        result = db.execute_raw(sql)
        print(f"Result: {result}")
        if result and isinstance(result, list):
            print(f"Number of rows: {len(result)}")
            if len(result) > 0:
                print(f"First row: {result[0]}")
    except Exception as e:
        print(f"Error: {e}")