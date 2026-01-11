"""Query Execution Engine"""

from .storage import StorageEngine

class QueryExecutor:
    """Executes parsed SQL queries"""
    
    def __init__(self, storage_engine=None):
        self.storage = storage_engine or StorageEngine()
    
    def execute(self, parsed_query):
        query_type = parsed_query['type']
        
        if query_type == 'CREATE_TABLE':
            return self.storage.create_table(
                parsed_query['table_name'],
                parsed_query['columns'],
                parsed_query['primary_key'],
                parsed_query['unique_keys']
            )
        
        elif query_type == 'INSERT':
            return self.storage.insert(
                parsed_query['table_name'],
                parsed_query['values']
            )
        
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