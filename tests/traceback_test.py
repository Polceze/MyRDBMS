import sys
sys.path.append('.')

print("Getting full traceback...")

from rdbms.executor import QueryExecutor
import traceback

db = QueryExecutor('traceback.db')

try:
    result = db.execute_raw("CREATE TABLE trace_test (id INT, name VARCHAR(50))")
    print(f"✅ Success: {result}")
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nFull traceback:")
    traceback.print_exc()
    
    # Also print the parsed query
    print("\n" + "="*60)
    print("Let's see what the parser returns...")
    from rdbms.parser import SQLParser
    try:
        parsed = SQLParser.parse("CREATE TABLE trace_test (id INT, name VARCHAR(50))")
        print(f"Parsed: {parsed}")
    except Exception as e2:
        print(f"Parser error: {e2}")