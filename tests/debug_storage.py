import sys
sys.path.append('.')

print("Testing StorageEngine directly...")

from rdbms.storage import StorageEngine
import os

# Clean up
if os.path.exists('debug_storage.db'):
    os.remove('debug_storage.db')

# Create storage engine
storage = StorageEngine('debug_storage.db')

print("\n1. Testing create_table with simple columns...")
try:
    # Direct test bypassing parser
    columns = [
        ('id', 'INT', None),  # (name, type, constraint)
        ('name', 'VARCHAR(50)', None)
    ]
    
    result = storage.create_table('direct_test', columns, primary_key='id')
    print(f"✅ Direct create_table: {result}")
    
except Exception as e:
    print(f"❌ Error in create_table: {e}")
    import traceback
    traceback.print_exc()

print("\n2. Testing with the exact output from parser...")
try:
    # Exactly what parser returns
    parsed_columns = [('id', 'INT', None), ('name', 'VARCHAR(50)', None)]
    
    result = storage.create_table('parser_test', parsed_columns, primary_key=None, unique_keys=[])
    print(f"✅ Parser-style create_table: {result}")
    
except Exception as e:
    print(f"❌ Error with parser output: {e}")
    import traceback
    traceback.print_exc()

print("\n3. Let's check DataType.validate...")
try:
    from rdbms.types import DataType
    
    print("   Testing INT validation...")
    result = DataType.validate(42, 'INT')
    print(f"   ✅ INT(42) -> {result}")
    
    print("   Testing VARCHAR validation...")
    result = DataType.validate('test', 'VARCHAR(50)', max_length=50)
    print(f"   ✅ VARCHAR('test', 50) -> {result}")
    
    print("   Testing VARCHAR without max_length...")
    result = DataType.validate('test', 'VARCHAR')
    print(f"   ✅ VARCHAR('test') -> {result}")
    
except Exception as e:
    print(f"❌ Error in DataType: {e}")
    import traceback
    traceback.print_exc()