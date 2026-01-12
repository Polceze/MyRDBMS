import sys
sys.path.append('.')
from rdbms.storage import StorageEngine

storage = StorageEngine('instance/web_database.db')

# Get some enrollments
enrollments = storage.data.get('enrollments', [])
print(f"Number of enrollments: {len(enrollments)}")

# Try a simple join manually
if enrollments:
    rows = [enrollments[0]]  # Take first enrollment
    joined = storage._apply_join(rows, "INNER JOIN students ON enrollments.student_id = students.student_id")
    if joined:
        print(f"\nJoined row keys: {list(joined[0].keys())}")
        print(f"Joined row: {joined[0]}")