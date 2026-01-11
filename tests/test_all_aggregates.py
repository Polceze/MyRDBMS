import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing all aggregate functions...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

# First, add some numeric data for testing
print("\nAdding test data for aggregate functions...")
try:
    # Add ages to students for testing
    db.execute_raw("UPDATE students SET age = 20 WHERE student_id = 1")
    db.execute_raw("UPDATE students SET age = 22 WHERE student_id = 2")
    db.execute_raw("UPDATE students SET age = 25 WHERE student_id = 3")
    db.execute_raw("UPDATE students SET age = 19 WHERE student_id = 4")
    db.execute_raw("UPDATE students SET age = 21 WHERE student_id = 5")
    print("Added age data for testing")
except:
    print("Could not add age data (column might not exist)")

test_queries = [
    # Basic aggregates
    ("SELECT SUM(enrollment_year) FROM students", "SUM of enrollment years"),
    ("SELECT AVG(enrollment_year) FROM students", "AVG of enrollment years"),
    ("SELECT MIN(enrollment_year) FROM students", "MIN enrollment year"),
    ("SELECT MAX(enrollment_year) FROM students", "MAX enrollment year"),
    
    # With aliases
    ("SELECT SUM(enrollment_year) as total_years FROM students", "SUM with alias"),
    ("SELECT AVG(enrollment_year) as avg_year FROM students", "AVG with alias"),
    
    # Multiple aggregates
    ("SELECT COUNT(*), AVG(enrollment_year) FROM students", "COUNT and AVG together"),
    ("SELECT MIN(enrollment_year) as min_year, MAX(enrollment_year) as max_year FROM students", "MIN and MAX with aliases"),
    
    # Test with WHERE
    ("SELECT AVG(enrollment_year) FROM students WHERE enrollment_year > 2021", "AVG with WHERE"),
]

print("\n" + "="*60)
for sql, description in test_queries:
    print(f"\n{description}:")
    print(f"SQL: {sql}")
    try:
        result = db.execute_raw(sql)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()