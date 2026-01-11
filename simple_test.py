"""Quick test to verify the RDBMS works"""

import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor

# Clean database file
import os
if os.path.exists('test.db'):
    os.remove('test.db')

db = QueryExecutor('test.db')

print("Testing SimpleRDBMS...")

# Test 1: Simple CREATE TABLE
print("\n1. Creating table...")
try:
    result = db.execute_raw('''
        CREATE TABLE students (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            age INT
        )
    ''')
    print(f"✅ Table created: {result}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 2: INSERT
print("\n2. Inserting data...")
try:
    result = db.execute_raw("INSERT INTO students VALUES (1, 'Alice', 20)")
    print(f"✅ Inserted: {result}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 3: SELECT
print("\n3. Selecting data...")
try:
    result = db.execute_raw("SELECT * FROM students")
    print(f"✅ Selected: {result}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n✅ Basic test complete!")