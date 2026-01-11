import sys
sys.path.append('.')
from rdbms.storage import StorageEngine
import os

print("Debugging column selection after JOIN...")

# Create a minimal test
if os.path.exists('debug_select.db'):
    os.remove('debug_select.db')

storage = StorageEngine('debug_select.db')

# Create test data
students_columns = [
    {'name': 'student_id', 'type': 'INT', 'max_length': None, 'nullable': False},
    {'name': 'first_name', 'type': 'VARCHAR', 'max_length': 50, 'nullable': True},
]

storage.schema['students'] = {
    'columns': students_columns,
    'primary_key': 'student_id',
    'unique_keys': [],
    'indexes': []
}
storage.data['students'] = [
    {'student_id': 1, 'first_name': 'John', '_rowid': 0},
]
storage.row_counter['students'] = 1

enrollments_columns = [
    {'name': 'enrollment_id', 'type': 'INT', 'max_length': None, 'nullable': False},
    {'name': 'student_id', 'type': 'INT', 'max_length': None, 'nullable': True},
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
    {'enrollment_id': 1, 'student_id': 1, 'enrollment_date': '2023-09-01', 'grade': 'A', '_rowid': 0},
]
storage.row_counter['enrollments'] = 1

print("\n1. Testing JOIN directly...")
# Simulate what happens in select()
rows = storage.data['enrollments']
join_clause = "INNER JOIN students ON enrollments.student_id = students.student_id"

joined_rows = storage._apply_join(rows, join_clause)
print(f"Joined rows: {joined_rows}")
if joined_rows and len(joined_rows) > 0:
    print(f"Joined row keys: {list(joined_rows[0].keys())}")

print("\n2. Testing column selection logic...")
# Test the column matching logic
test_columns = [
    'students.first_name',
    'first_name', 
    'enrollment_date',
    'grade'
]

for col_spec in test_columns:
    print(f"\nLooking for column: '{col_spec}'")
    row = joined_rows[0] if joined_rows else {}
    
    # Try exact match
    if col_spec in row:
        print(f"  ✓ Exact match: {row[col_spec]}")
    else:
        print(f"  ✗ No exact match")
    
    # Try with table prefix logic
    if '.' in col_spec:
        col_name_only = col_spec.split('.')[-1]
        print(f"  Looking for key ending with '.{col_name_only}'")
        for key in row.keys():
            if key.endswith('.' + col_name_only):
                print(f"  ✓ Found '{key}': {row[key]}")
                break
        else:
            print(f"  ✗ No match ending with '.{col_name_only}'")
    
    # Try column name without prefix
    col_name_only = col_spec.split('.')[-1] if '.' in col_spec else col_spec
    if col_name_only in row:
        print(f"  ✓ Found '{col_name_only}' without prefix: {row[col_name_only]}")
    else:
        print(f"  ✗ No match for '{col_name_only}' without prefix")

print("\n3. Testing the actual select method...")
result = storage.select('enrollments', 
                       columns='students.first_name, enrollment_date, grade',
                       join='INNER JOIN students ON enrollments.student_id = students.student_id')
print(f"Select result: {result}")