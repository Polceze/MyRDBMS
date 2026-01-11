"""Query Execution Engine"""

from .storage import StorageEngine

class QueryExecutor:
    
    def __init__(self, db_file='database.db'):
        # Create StorageEngine instance
        self.storage = StorageEngine(db_file)
    
    def execute(self, parsed_query):
        """Execute a parsed query"""
        query_type = parsed_query['type']
        
        if query_type == 'CREATE_TABLE':
            return self.storage.create_table(
                parsed_query['table_name'],
                parsed_query['columns'],
                parsed_query['primary_key'],
                parsed_query['unique_keys']
            )
        
        elif query_type == 'INSERT':
            values_dict = parsed_query['values']
            table_name = parsed_query['table_name']
            
            # Check if this is a positional INSERT (no column names specified)
            if parsed_query.get('is_positional', False):
                # Get the positional values list
                positional_values = values_dict['__positional_values']
                
                # Check if table exists
                if table_name not in self.storage.schema:
                    raise ValueError(f"Table '{table_name}' doesn't exist")
                
                # Get column names in order from table schema
                table_schema = self.storage.schema[table_name]
                column_names = [col['name'] for col in table_schema['columns']]
                
                # Check if we have the right number of values
                if len(positional_values) != len(column_names):
                    raise ValueError(
                        f"Table '{table_name}' has {len(column_names)} columns "
                        f"but {len(positional_values)} values were provided"
                    )
                
                # Map positional values to column names
                mapped_values = {}
                for i, col_name in enumerate(column_names):
                    mapped_values[col_name] = positional_values[i]
                
                # Insert with mapped values
                return self.storage.insert(table_name, mapped_values)
            else:
                # Regular INSERT with column names
                return self.storage.insert(table_name, values_dict)
        
        elif query_type == 'SELECT':
            return self.storage.select(
                parsed_query['table_name'],
                parsed_query['columns'],
                parsed_query.get('where'),
                parsed_query.get('join')
            )
        
        elif query_type == 'UPDATE':
            return self.storage.update(
                parsed_query['table_name'],
                parsed_query['set_values'],
                parsed_query.get('where')
            )
        
        elif query_type == 'DELETE':
            return self.storage.delete(
                parsed_query['table_name'],
                parsed_query.get('where')
            )
        
        elif query_type == 'CREATE_INDEX':
            return self.storage.create_index(
                parsed_query['table_name'],
                parsed_query['column_name']
            )
        
        else:
            raise ValueError(f"Unknown query type: {query_type}")
    
    def execute_raw(self, sql):
        """Parse and execute raw SQL"""
        from .parser import SQLParser
        parsed = SQLParser.parse(sql)
        return self.execute(parsed)