import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Debugging aggregate function handling...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

# Test different types of queries
test_queries = [
    # Basic COUNT
    ("SELECT COUNT(*) FROM students", "Basic COUNT(*)"),
    
    # COUNT with alias
    ("SELECT COUNT(*) as total FROM students", "COUNT with alias"),
    
    # COUNT specific column
    ("SELECT COUNT(student_id) FROM students", "COUNT(column)"),
    
    # Multiple aggregates
    ("SELECT COUNT(*), AVG(enrollment_year) FROM students", "Multiple aggregates"),
    
    # Mix aggregates with regular columns (invalid in SQL but let's test)
    ("SELECT first_name, COUNT(*) FROM students", "Mixed query"),
    
    # Group by (if supported)
    ("SELECT enrollment_year, COUNT(*) FROM students GROUP BY enrollment_year", "GROUP BY"),
]

for sql, description in test_queries:
    print(f"\n{description}:")
    print(f"  SQL: {sql}")
    try:
        result = db.execute_raw(sql)
        print(f"  Result: {result}")
        if result and isinstance(result, list) and len(result) > 0:
            print(f"  First row type: {type(result[0])}")
            if isinstance(result[0], dict):
                print(f"  Keys: {list(result[0].keys())}")
                print(f"  Values: {list(result[0].values())}")
    except Exception as e:
        print(f"  Error: {e}")