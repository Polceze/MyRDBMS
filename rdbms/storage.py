import os
import pickle
from collections import defaultdict
from .types import DataType

# Storage Engine for RDBMS
class StorageEngine:
    
    def __init__(self, db_file='database.db'):
        """Initialize storage engine"""
        self.db_file = db_file
        self.schema = {}  # table_name -> {columns: [], primary_key: None, indexes: {}}
        self.data = defaultdict(list)  # table_name -> list of rows
        self.indexes = defaultdict(dict)  # table_name -> {column: {value: [row_ids]}}
        self.row_counter = defaultdict(int)  # table_name -> next row_id
        
        self.load()
    
    def load(self):
        """Load database from file"""
        if os.path.exists(self.db_file):
            with open(self.db_file, 'rb') as f:
                data = pickle.load(f)
                self.schema = data.get('schema', {})
                self.data = data.get('data', defaultdict(list))
                self.indexes = data.get('indexes', defaultdict(dict))
                self.row_counter = data.get('row_counter', defaultdict(int))
    
    def save(self):
        """Save database to file"""
        with open(self.db_file, 'wb') as f:
            pickle.dump({
                'schema': self.schema,
                'data': dict(self.data),
                'indexes': dict(self.indexes),
                'row_counter': dict(self.row_counter)
            }, f)
    
    def create_table(self, table_name, columns, primary_key=None, unique_keys=None):
        """Create a new table"""
        if table_name in self.schema:
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Validate column definitions
        validated_columns = []
        for col_def in columns:
            if len(col_def) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            
            col_name = col_def[0]
            col_type = col_def[1].upper()
            max_length = None
            
            if col_type.startswith('VARCHAR'):
                if '(' in col_type:
                    try:
                        max_length = int(col_type.split('(')[1].split(')')[0])
                        col_type = 'VARCHAR'
                    except:
                        raise ValueError(f"Invalid VARCHAR length in: {col_type}")
            
            # Validate data type
            if col_type not in [DataType.INT, DataType.VARCHAR, DataType.TEXT, 
                            DataType.DATE, DataType.FLOAT, DataType.BOOL]:
                raise ValueError(f"Unsupported data type: {col_type}")
            
            # Handle nullable constraint
            nullable = True  # Default is nullable
            if len(col_def) >= 3 and col_def[2] is not None:
                # If there's a constraint, check if it's NOT NULL
                constraint = col_def[2].upper()
                nullable = constraint != 'NOT NULL'
            
            validated_columns.append({
                'name': col_name,
                'type': col_type,
                'max_length': max_length,
                'nullable': nullable  # Use the calculated value
            })
        
        # Store schema
        self.schema[table_name] = {
            'columns': validated_columns,
            'primary_key': primary_key,
            'unique_keys': unique_keys or [],
            'indexes': []
        }
        
        # Initialize empty data
        self.data[table_name] = []
        self.row_counter[table_name] = 0
        
        self.save()
        return True
    
    def insert(self, table_name, values_dict):
        """Insert a row into table"""
        if table_name not in self.schema:
            raise ValueError(f"Table '{table_name}' doesn't exist")
        
        schema = self.schema[table_name]
        row = {}
        
        # Validate and prepare row
        for col in schema['columns']:
            col_name = col['name']
            if col_name in values_dict:
                value = DataType.validate(
                    values_dict[col_name],
                    col['type'],
                    col['max_length']
                )
                
                # Check NOT NULL constraint
                if value is None and not col['nullable']:
                    raise ValueError(f"Column '{col_name}' cannot be NULL")
                
                row[col_name] = value
            else:
                if not col['nullable']:
                    raise ValueError(f"Missing required column: {col_name}")
                row[col_name] = None
        
        # Check primary key uniqueness
        if schema['primary_key']:
            pk_col = schema['primary_key']
            if pk_col in row:
                pk_value = row[pk_col]
                if pk_value is None:
                    raise ValueError(f"Primary key '{pk_col}' cannot be NULL")
                
                # Check if PK already exists
                pk_index = self.indexes[table_name].get(pk_col, {})
                if pk_value in pk_index:
                    raise ValueError(f"Duplicate primary key value: {pk_value}")
        
        # Check unique constraints
        for unique_col in schema['unique_keys']:
            if unique_col in row and row[unique_col] is not None:
                unique_index = self.indexes[table_name].get(unique_col, {})
                if row[unique_col] in unique_index:
                    raise ValueError(f"Duplicate unique value for '{unique_col}': {row[unique_col]}")
        
        # Add row_id
        row_id = self.row_counter[table_name]
        row['_rowid'] = row_id
        self.data[table_name].append(row)
        self.row_counter[table_name] += 1
        
        # Update indexes
        for col_name in row:
            if col_name != '_rowid':
                if col_name not in self.indexes[table_name]:
                    self.indexes[table_name][col_name] = {}
                if row[col_name] not in self.indexes[table_name][col_name]:
                    self.indexes[table_name][col_name][row[col_name]] = []
                self.indexes[table_name][col_name][row[col_name]].append(row_id)
        
        self.save()
        return row_id
    
    def select(self, table_name, columns='*', where=None, join=None):
        """Select rows from table with optional WHERE and JOIN"""
        if table_name not in self.schema:
            raise ValueError(f"Table '{table_name}' doesn't exist")
        
        rows = self.data[table_name]
        
        # Apply WHERE clause if provided
        if where:
            rows = self._apply_where(rows, where)
        
        # Apply JOIN if provided
        if join:
            rows = self._apply_join(rows, join)
        
        # Select specific columns
        if columns != '*':
            selected_rows = []
            for row in rows:
                selected_row = {}
                for col in columns.split(','):
                    col = col.strip()
                    if col in row:
                        selected_row[col] = row[col]
                selected_rows.append(selected_row)
            rows = selected_rows
        
        return rows
    
    def _apply_where(self, rows, where_clause):
        """Simple WHERE clause evaluation"""
        filtered = []
        for row in rows:
            if self._evaluate_condition(row, where_clause):
                filtered.append(row)
        return filtered
    
    def _evaluate_condition(self, row, condition):
        """Evaluate a condition like "age > 18 AND name = 'John'"""
        # Implementation for basic conditions
        if ' AND ' in condition:
            parts = condition.split(' AND ')
            return all(self._evaluate_condition(row, part) for part in parts)
        elif ' OR ' in condition:
            parts = condition.split(' OR ')
            return any(self._evaluate_condition(row, part) for part in parts)
        elif '=' in condition:
            col, value = condition.split('=', 1)
            col = col.strip()
            value = value.strip().strip("'")
            return str(row.get(col)) == value
        elif '>' in condition:
            col, value = condition.split('>', 1)
            col = col.strip()
            value = value.strip().strip("'")
            try:
                return float(row.get(col)) > float(value)
            except:
                return False
        elif '<' in condition:
            col, value = condition.split('<', 1)
            col = col.strip()
            value = value.strip().strip("'")
            try:
                return float(row.get(col)) < float(value)
            except:
                return False
        return True
    
    def _apply_join(self, rows, join_clause):
        """Simple INNER JOIN implementation"""
        # Format: "INNER JOIN other_table ON table.col = other_table.col"
        parts = join_clause.split()
        if len(parts) < 5:
            return rows
        
        join_type = parts[0] + ' ' + parts[1]  # "INNER JOIN"
        other_table = parts[2]
        on_clause = ' '.join(parts[3:])
        
        if other_table not in self.data:
            return rows
        
        # Parse ON clause: "table.col = other_table.col"
        if '=' in on_clause:
            left, right = on_clause.split('=')
            left = left.strip().replace('ON ', '')
            right = right.strip()
            
            left_table, left_col = left.split('.')
            right_table, right_col = right.split('.')
            
            # Build index of other table for faster join
            other_rows = self.data[other_table]
            other_index = {}
            for row in other_rows:
                key = row.get(right_col)
                if key not in other_index:
                    other_index[key] = []
                other_index[key].append(row)
            
            # Perform join
            joined_rows = []
            for left_row in rows:
                key = left_row.get(left_col)
                if key in other_index:
                    for right_row in other_index[key]:
                        # Merge rows
                        merged = left_row.copy()
                        for k, v in right_row.items():
                            if k not in merged:
                                merged[f"{other_table}.{k}"] = v
                        joined_rows.append(merged)
            
            return joined_rows
        
        return rows
    
    def update(self, table_name, set_values, where=None):
        """Update rows in table"""
        rows = self.select(table_name, where=where)
        updated_count = 0
        
        for row in rows:
            row_id = row['_rowid']
            # Update the row in data
            for i, data_row in enumerate(self.data[table_name]):
                if data_row['_rowid'] == row_id:
                    # Remove old values from indexes
                    for col_name in set_values:
                        old_value = data_row.get(col_name)
                        if old_value in self.indexes[table_name].get(col_name, {}):
                            self.indexes[table_name][col_name][old_value].remove(row_id)
                    
                    # Update row
                    for col_name, value in set_values.items():
                        self.data[table_name][i][col_name] = value
                        # Add to index
                        if col_name not in self.indexes[table_name]:
                            self.indexes[table_name][col_name] = {}
                        if value not in self.indexes[table_name][col_name]:
                            self.indexes[table_name][col_name][value] = []
                        self.indexes[table_name][col_name][value].append(row_id)
                    
                    updated_count += 1
                    break
        
        if updated_count > 0:
            self.save()
        
        return updated_count
    
    def delete(self, table_name, where=None):
        """Delete rows from table"""
        rows = self.select(table_name, where=where)
        deleted_count = 0
        
        for row in rows:
            row_id = row['_rowid']
            # Find and remove row
            for i, data_row in enumerate(self.data[table_name]):
                if data_row['_rowid'] == row_id:
                    # Remove from indexes
                    for col_name, value in data_row.items():
                        if col_name != '_rowid' and value in self.indexes[table_name].get(col_name, {}):
                            self.indexes[table_name][col_name][value].remove(row_id)
                    
                    # Remove row
                    self.data[table_name].pop(i)
                    deleted_count += 1
                    break
        
        if deleted_count > 0:
            self.save()
        
        return deleted_count
    
    def create_index(self, table_name, column_name):
        """Create an index on a column"""
        if table_name not in self.schema:
            raise ValueError(f"Table '{table_name}' doesn't exist")
        
        # Build index
        if column_name not in self.indexes[table_name]:
            self.indexes[table_name][column_name] = {}
        
        for row in self.data[table_name]:
            value = row.get(column_name)
            row_id = row['_rowid']
            if value not in self.indexes[table_name][column_name]:
                self.indexes[table_name][column_name][value] = []
            if row_id not in self.indexes[table_name][column_name][value]:
                self.indexes[table_name][column_name][value].append(row_id)
        
        # Add to schema
        if column_name not in self.schema[table_name]['indexes']:
            self.schema[table_name]['indexes'].append(column_name)
        
        self.save()
        return True