import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing enrollment query structure...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

# Test the simple query
print("\n1. Testing simple enrollment query:")
sql = '''
    SELECT enrollments.enrollment_id, enrollments.student_id, 
           enrollments.course_id, enrollments.enrollment_date,
           enrollments.grade
    FROM enrollments
    ORDER BY enrollments.enrollment_date DESC
    LIMIT 2
'''
print(f"SQL: {sql}")
try:
    result = db.execute_raw(sql)
    print(f"Result: {result}")
    if result and len(result) > 0:
        print(f"First row keys: {list(result[0].keys())}")
        print(f"First row values: {result[0]}")
except Exception as e:
    print(f"Error: {e}")

# Test without table prefixes
print("\n2. Testing without table prefixes:")
sql = '''
    SELECT enrollment_id, student_id, course_id, enrollment_date, grade
    FROM enrollments
    ORDER BY enrollment_date DESC
    LIMIT 2
'''
print(f"SQL: {sql}")
try:
    result = db.execute_raw(sql)
    print(f"Result: {result}")
    if result and len(result) > 0:
        print(f"First row keys: {list(result[0].keys())}")
except Exception as e:
    print(f"Error: {e}")

# Test what the parser returns
print("\n3. Testing parser output:")
from rdbms.parser import SQLParser
try:
    parsed = SQLParser.parse(sql)
    print(f"Parsed: {parsed}")
except Exception as e:
    print(f"Parser error: {e}")