import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor

db = QueryExecutor('instance/web_database.db')

print("Checking result formats...")

# Test 1: COUNT query
print("\n1. Testing COUNT(*):")
result = db.execute_raw('SELECT COUNT(*) FROM students')
print(f"Result: {result}")
if result and isinstance(result, list) and len(result) > 0:
    print(f"First row: {result[0]}")
    print(f"Keys: {list(result[0].keys())}")

# Test 2: Regular SELECT
print("\n2. Testing regular SELECT:")
result = db.execute_raw('SELECT student_id, first_name FROM students LIMIT 2')
print(f"Result: {result}")
if result and isinstance(result, list) and len(result) > 0:
    print(f"First row keys: {list(result[0].keys())}")

# Test 3: SELECT with alias
print("\n3. Testing SELECT with alias:")
result = db.execute_raw('SELECT COUNT(*) as student_count FROM students')
print(f"Result: {result}")
if result and isinstance(result, list) and len(result) > 0:
    print(f"First row: {result[0]}")