import sys
sys.path.append('.')
from rdbms.parser import SQLParser
from rdbms.executor import QueryExecutor

print("Testing parser directly...")

sql = """
CREATE TABLE students (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT
)
"""

print(f"SQL: {sql}")

# Test parser
try:
    parsed = SQLParser.parse(sql)
    print(f"\n✅ Parser returned: {type(parsed)}")
    print(f"Parsed data: {parsed}")
except Exception as e:
    print(f"\n❌ Parser error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("Testing executor...")

db = QueryExecutor('debug.db')

try:
    result = db.execute_raw(sql)
    print(f"\n✅ Executor returned: {type(result)}")
    print(f"Result: {result}")
except Exception as e:
    print(f"\n❌ Executor error: {e}")
    import traceback
    traceback.print_exc()