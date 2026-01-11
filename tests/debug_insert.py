import sys
sys.path.append('.')
from rdbms.parser import SQLParser
from rdbms.executor import QueryExecutor
import os

print("Debugging INSERT issue...")

if os.path.exists('debug_insert.db'):
    os.remove('debug_insert.db')

db = QueryExecutor('debug_insert.db')

# Create table
print("\n1. Creating table...")
create_sql = """
CREATE TABLE students (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT
)
"""
db.execute_raw(create_sql)
print("✅ Table created")

# Test INSERT parsing
print("\n2. Testing INSERT parser...")
insert_sql = "INSERT INTO students VALUES (1, 'Alice', 20)"
print(f"SQL: {insert_sql}")

try:
    parsed = SQLParser.parse(insert_sql)
    print(f"✅ Parsed: {parsed}")
    
    # Check what values_dict looks like
    print(f"  Table: {parsed['table_name']}")
    print(f"  Values dict: {parsed['values']}")
    
except Exception as e:
    print(f"❌ Parser error: {e}")
    import traceback
    traceback.print_exc()

# Test actual execution
print("\n3. Testing INSERT execution...")
try:
    result = db.execute_raw(insert_sql)
    print(f"✅ Insert result: {result}")
except Exception as e:
    print(f"❌ Insert error: {e}")
    import traceback
    traceback.print_exc()

# Test with column names specified
print("\n4. Testing INSERT with column names...")
insert_with_cols = "INSERT INTO students (id, name, age) VALUES (2, 'Bob', 25)"
try:
    parsed = SQLParser.parse(insert_with_cols)
    print(f"✅ Parsed: {parsed['values']}")
    
    result = db.execute_raw(insert_with_cols)
    print(f"✅ Insert result: {result}")
except Exception as e:
    print(f"❌ Error: {e}")