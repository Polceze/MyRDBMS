import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing positional INSERT fix...")

# Clean up
if os.path.exists('test_pos.db'):
    os.remove('test_pos.db')

db = QueryExecutor('test_pos.db')

# Create table
print("\n1. Creating table...")
db.execute_raw("""
    CREATE TABLE students (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        age INT
    )
""")
print("✅ Table created")

# Test positional INSERT (no column names)
print("\n2. Testing positional INSERT (no column names)...")
try:
    result = db.execute_raw("INSERT INTO students VALUES (1, 'Alice', 20)")
    print(f"✅ Positional INSERT worked! Row ID: {result}")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

# Test INSERT with column names
print("\n3. Testing INSERT with column names...")
try:
    result = db.execute_raw("INSERT INTO students (id, name, age) VALUES (2, 'Bob', 25)")
    print(f"✅ Named INSERT worked! Row ID: {result}")
except Exception as e:
    print(f"❌ Error: {e}")

# Select data
print("\n4. Selecting data...")
try:
    rows = db.execute_raw("SELECT * FROM students")
    print(f"✅ Found {len(rows)} students:")
    for row in rows:
        print(f"   - ID: {row['id']}, Name: {row['name']}, Age: {row['age']}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test error cases
print("\n5. Testing error cases...")

# Too few values
print("   Testing too few values...")
try:
    db.execute_raw("INSERT INTO students VALUES (3, 'Charlie')")  # Missing age
    print("   ❌ Should have failed!")
except Exception as e:
    print(f"   ✅ Correctly failed: {str(e)[:50]}...")

# Too many values
print("   Testing too many values...")
try:
    db.execute_raw("INSERT INTO students VALUES (4, 'David', 30, 'Extra')") # Extra value
    print("   ❌ Should have failed!")
except Exception as e:
    print(f"   ✅ Correctly failed: {str(e)[:50]}...")

print("\n✅ Test complete!")