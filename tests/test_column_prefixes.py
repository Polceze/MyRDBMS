import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing column name handling with/without table prefixes...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

# Test queries with and without table prefixes
test_queries = [
    # Without prefixes (should work)
    ('''SELECT first_name, last_name, enrollment_date, grade
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        INNER JOIN courses ON enrollments.course_id = courses.course_id
        LIMIT 2''',
     "Without table prefixes"),
    
    # With prefixes (might fail without fix)
    ('''SELECT students.first_name, students.last_name, 
               enrollments.enrollment_date, enrollments.grade
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        INNER JOIN courses ON enrollments.course_id = courses.course_id
        LIMIT 2''',
     "With table prefixes"),
    
    # Mix of both
    ('''SELECT students.first_name, last_name, 
               enrollment_date, enrollments.grade
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        LIMIT 2''',
     "Mixed prefixes"),
]

print("\n" + "="*60)
for sql, description in test_queries:
    print(f"\n{description}:")
    print(f"SQL: {sql[:80]}...")
    try:
        result = db.execute_raw(sql)
        print(f"Success! Returned {len(result)} rows")
        if result and len(result) > 0:
            print(f"First row keys: {list(result[0].keys())}")
            print(f"First row: {result[0]}")
    except Exception as e:
        print(f"Error: {e}")