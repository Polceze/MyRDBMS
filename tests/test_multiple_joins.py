import sys
sys.path.append('.')
from rdbms.storage import StorageEngine

storage = StorageEngine('instance/web_database.db')

# Test _apply_join with a single JOIN
print("Testing single JOIN...")
enrollments = storage.data.get('enrollments', [])
if enrollments:
    rows = [enrollments[0]]
    result = storage._apply_join(rows, "INNER JOIN students ON enrollments.student_id = students.student_id")
    print(f"Result after single JOIN: {len(result)} rows")
    if result:
        print(f"Keys in result: {list(result[0].keys())}")

# Test _apply_join with multiple JOINs
print("\nTesting multiple JOINs...")
if enrollments:
    rows = [enrollments[0]]
    result = storage._apply_join(rows, "INNER JOIN students ON enrollments.student_id = students.student_id INNER JOIN courses ON enrollments.course_id = courses.course_id")
    print(f"Result after multiple JOINs: {len(result)} rows")
    if result:
        print(f"Keys in result: {list(result[0].keys())}")