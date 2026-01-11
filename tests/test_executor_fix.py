import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor

print("Testing QueryExecutor initialization...")

# Test 1: Check if storage is StorageEngine instance
db = QueryExecutor('test_executor.db')
print(f"1. Executor created: {type(db)}")
print(f"   Storage attribute type: {type(db.storage)}")

# If storage is a string, that's an error
if isinstance(db.storage, str):
    print("   ❌ ERROR: storage is a string, not StorageEngine!")
    print("   Fix your QueryExecutor.__init__ method")
elif hasattr(db.storage, 'create_table'):
    print("   ✅ Good: storage has create_table method")
else:
    print(f"   ⚠️  Unexpected: storage is {type(db.storage)}")

# Test 2: Try to execute SQL
print("\n2. Testing SQL execution...")
try:
    result = db.execute_raw("CREATE TABLE test_exec (id INT, name VARCHAR(50))")
    print(f"   ✅ SQL executed: {result}")
    
    # Try insert
    db.execute_raw("INSERT INTO test_exec VALUES (1, 'Test User')")
    print("   ✅ Insert worked")
    
    # Try select
    rows = db.execute_raw("SELECT * FROM test_exec")
    print(f"   ✅ Select returned {len(rows)} rows")
    
except AttributeError as e:
    if "'str' object has no attribute" in str(e):
        print(f"   ❌ ERROR: storage is still a string!")
        print(f"   Error: {e}")
    else:
        print(f"   ❌ Other attribute error: {e}")
except Exception as e:
    print(f"   ❌ Error: {type(e).__name__}: {e}")

print("\n✅ Test complete!")