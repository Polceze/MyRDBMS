import sys
sys.path.append('.')
from rdbms.executor import QueryExecutor
import os

print("Testing JOIN queries...")

db_path = 'instance/web_database.db'
if not os.path.exists(db_path):
    print(f"Database file not found: {db_path}")
    exit(1)

db = QueryExecutor(db_path)

test_queries = [
    # Simple JOIN
    ('''SELECT students.first_name, courses.course_name
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        INNER JOIN courses ON enrollments.course_id = courses.course_id
        LIMIT 3''',
     "Simple JOIN"),
    
    # JOIN with WHERE
    ('''SELECT students.first_name, courses.course_name, enrollments.grade
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        INNER JOIN courses ON enrollments.course_id = courses.course_id
        WHERE enrollments.grade = 'A'
        LIMIT 3''',
     "JOIN with WHERE"),
    
    # JOIN with ORDER BY
    ('''SELECT students.first_name, courses.course_name, enrollments.enrollment_date
        FROM enrollments
        INNER JOIN students ON enrollments.student_id = students.student_id
        INNER JOIN courses ON enrollments.course_id = courses.course_id
        ORDER BY enrollments.enrollment_date DESC
        LIMIT 2''',
     "JOIN with ORDER BY"),
]

print("\n" + "="*60)
for sql, description in test_queries:
    print(f"\n{description}:")
    print(f"SQL: {sql[:100]}...")
    try:
        result = db.execute_raw(sql)
        print(f"Success! Returned {len(result)} rows")
        if result and len(result) > 0:
            print(f"First row: {result[0]}")
    except Exception as e:
        print(f"Error: {e}")
        # Try to parse it to see if something's wrong
        try:
            from rdbms.parser import SQLParser
            parsed = SQLParser.parse(sql)
            print(f"Parsed successfully: {parsed}")
        except Exception as e2:
            print(f"Parser error: {e2}")