import sys
sys.path.append('.')

# First, we trace the INSERT parsing
sql = "INSERT INTO students VALUES (1, 'Alice', 20)"
print(f"Testing SQL: {sql}")

from rdbms.parser import SQLParser
try:
    parsed = SQLParser.parse(sql)
    print(f"\nParsed result:")
    print(f"  Type: {parsed['type']}")
    print(f"  Table: {parsed['table_name']}")
    print(f"  Values dict: {parsed['values']}")
    
    # What does storage engine expect?
    print("\nNow let's trace through storage engine...")
    
    from rdbms.storage import StorageEngine
    import os
    
    if os.path.exists('trace.db'):
        os.remove('trace.db')
    
    storage = StorageEngine('trace.db')
    
    # Create table
    columns = [('id', 'INT', None), ('name', 'VARCHAR(50)', None), ('age', 'INT', None)]
    storage.create_table('students', columns, primary_key='id')
    print("✅ Table created")
    
    # Try to insert with the parsed values
    print(f"\nTrying to insert with: {parsed['values']}")
    try:
        result = storage.insert('students', parsed['values'])
        print(f"✅ Insert result: {result}")
    except Exception as e:
        print(f"❌ Insert failed: {e}")
        print("\nLet's check what columns the table has...")
        print(f"Schema: {storage.schema['students']['columns']}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()