import sys
sys.path.append('.')
from rdbms.storage import StorageEngine
import os

print("Debugging JOIN execution...")

# Create a minimal test database
if os.path.exists('test_join.db'):
    os.remove('test_join.db')

storage = StorageEngine('test_join.db')

# Create simple test tables
# Students table
students_columns = [
    {'name': 'student_id', 'type': 'INT', 'max_length': None, 'nullable': False},
    {'name': 'first_name', 'type': 'VARCHAR', 'max_length': 50, 'nullable': True},
    {'name': 'last_name', 'type': 'VARCHAR', 'max_length': 50, 'nullable': True},
]

storage.schema['students'] = {
    'columns': students_columns,
    'primary_key': 'student_id',
    'unique_keys': [],
    'indexes': []
}
storage.data['students'] = [
    {'student_id': 1, 'first_name': 'John', 'last_name': 'Doe', '_rowid': 0},
    {'student_id': 2, 'first_name': 'Jane', 'last_name': 'Smith', '_rowid': 1},
]
storage.row_counter['students'] = 2

# Enrollments table
enrollments_columns = [
    {'name': 'enrollment_id', 'type': 'INT', 'max_length': None, 'nullable': False},
    {'name': 'student_id', 'type': 'INT', 'max_length': None, 'nullable': True},
    {'name': 'course_id', 'type': 'INT', 'max_length': None, 'nullable': True},
    {'name': 'enrollment_date', 'type': 'DATE', 'max_length': None, 'nullable': True},
    {'name': 'grade', 'type': 'VARCHAR', 'max_length': 2, 'nullable': True},
]

storage.schema['enrollments'] = {
    'columns': enrollments_columns,
    'primary_key': 'enrollment_id',
    'unique_keys': [],
    'indexes': []
}
storage.data['enrollments'] = [
    {'enrollment_id': 1, 'student_id': 1, 'course_id': 101, 
     'enrollment_date': '2023-09-01', 'grade': 'A', '_rowid': 0},
    {'enrollment_id': 2, 'student_id': 2, 'course_id': 102,
     'enrollment_date': '2023-09-01', 'grade': 'B', '_rowid': 1},
]
storage.row_counter['enrollments'] = 2

print("\n1. Testing direct storage select with JOIN...")
try:
    # Test the _apply_join method directly
    enrollments = storage.data['enrollments']
    join_clause = "INNER JOIN students ON enrollments.student_id = students.student_id"
    
    print(f"Enrollments data: {enrollments}")
    print(f"Students data: {storage.data['students']}")
    
    joined = storage._apply_join(enrollments, join_clause)
    print(f"Joined result: {joined}")
    if joined and len(joined) > 0:
        print(f"First joined row keys: {list(joined[0].keys())}")
        print(f"First joined row: {joined[0]}")
    
except Exception as e:
    print(f"Error in direct join test: {e}")
    import traceback
    traceback.print_exc()

print("\n2. Testing full select with JOIN...")
try:
    result = storage.select('enrollments', 
                           columns='first_name, last_name, enrollment_date, grade',
                           join='INNER JOIN students ON enrollments.student_id = students.student_id')
    print(f"Select with JOIN result: {result}")
    if result and len(result) > 0:
        print(f"First row keys: {list(result[0].keys())}")
except Exception as e:
    print(f"Error in select with JOIN: {e}")