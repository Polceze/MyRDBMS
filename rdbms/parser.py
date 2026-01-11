import re

# Parser for SQL commands
class SQLParser:
    
    @staticmethod
    def parse(sql):
        """Parse a SQL command and return its components"""
        # Removed uppercase conversion from here
        sql = sql.strip()
        
        sql_upper = sql.upper()
        if sql_upper.startswith('CREATE TABLE'):
            return SQLParser._parse_create_table(sql)
        elif sql_upper.startswith('INSERT INTO'):
            return SQLParser._parse_insert(sql)
        elif sql_upper.startswith('SELECT'):
            return SQLParser._parse_select(sql)
        elif sql_upper.startswith('UPDATE'):
            return SQLParser._parse_update(sql)
        elif sql_upper.startswith('DELETE FROM'):
            return SQLParser._parse_delete(sql)
        elif sql_upper.startswith('CREATE INDEX'):
            return SQLParser._parse_create_index(sql)
        else:
            raise ValueError(f"Unsupported SQL command: {sql}")
    
    @staticmethod
    def _parse_create_table(sql):
        # CREATE TABLE table_name (col1 TYPE, col2 TYPE, PRIMARY KEY(col))
        pattern = r'CREATE TABLE (\w+) \((.+)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid CREATE TABLE syntax: {sql}")
        
        table_name = match.group(1)
        columns_str = match.group(2).strip()
        
        print(f"[DEBUG] Parsing CREATE TABLE: {table_name}")
        print(f"[DEBUG] Columns string: {columns_str}")
        
        columns = []
        primary_key = None
        unique_keys = []
        
        # Token splitting that handles constraints properly
        tokens = []
        current = ""
        depth = 0
        
        # First pass: split by commas, respecting parentheses
        for char in columns_str:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                tokens.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            tokens.append(current.strip())
        
        # Second pass: separate column definitions from constraints
        column_tokens = []
        constraint_tokens = []
        
        for token in tokens:
            token_upper = token.upper()
            if token_upper.startswith('PRIMARY KEY') or token_upper.startswith('UNIQUE'):
                constraint_tokens.append(token)
            else:
                column_tokens.append(token)
        
        print(f"[DEBUG] Column tokens: {column_tokens}")
        print(f"[DEBUG] Constraint tokens: {constraint_tokens}")
        
        # Process column definitions
        for token in column_tokens:
            # Split into parts
            parts = token.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: {token}")
            
            col_name = parts[0]
            col_type = parts[1]
            
            # Check for PRIMARY KEY in the same token (e.g., "id INT PRIMARY KEY")
            # We need to handle this case
            if 'PRIMARY KEY' in token.upper():
                # Extract column type before PRIMARY KEY
                for i, part in enumerate(parts):
                    if part.upper() == 'PRIMARY':
                        # Everything before "PRIMARY" is column definition
                        col_type = parts[i-1] if i > 1 else parts[1]
                        # Set as primary key
                        if primary_key is None:
                            primary_key = col_name
                        else:
                            raise ValueError(f"Multiple primary keys defined: {primary_key} and {col_name}")
                        break
            
            # Handle NOT NULL
            nullable = True
            if 'NOT NULL' in token.upper():
                nullable = False
            
            # Handle VARCHAR length
            if '(' in col_type:
                # Keep as is (e.g., VARCHAR(50))
                pass
            
            columns.append((col_name, col_type, 'NOT NULL' if not nullable else None))
        
        # Process constraint tokens
        for token in constraint_tokens:
            token_upper = token.upper()
            
            if token_upper.startswith('PRIMARY KEY'):
                # PRIMARY KEY(col_name)
                pk_match = re.search(r'PRIMARY KEY\((\w+)\)', token, re.IGNORECASE)
                if pk_match:
                    if primary_key is None:
                        primary_key = pk_match.group(1)
                    else:
                        raise ValueError(f"Multiple primary keys defined: {primary_key} and {pk_match.group(1)}")
            
            elif token_upper.startswith('UNIQUE'):
                # UNIQUE(col_name) or UNIQUE KEY(col_name)
                unique_match = re.search(r'UNIQUE(?: KEY)?\((\w+)\)', token, re.IGNORECASE)
                if unique_match:
                    unique_keys.append(unique_match.group(1))
        
        print(f"[DEBUG] Parsed columns: {columns}")
        print(f"[DEBUG] Primary key: {primary_key}")
        print(f"[DEBUG] Unique keys: {unique_keys}")
        
        return {
            'type': 'CREATE_TABLE',
            'table_name': table_name,
            'columns': columns,
            'primary_key': primary_key,
            'unique_keys': unique_keys
        }
    
    @staticmethod
    def _parse_insert(sql):
        # Replace newlines with spaces
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # INSERT INTO table_name (col1, col2) VALUES ('val1', 'val2')
        pattern = r'INSERT INTO (\w+) \((.+?)\) VALUES \((.+)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            # Try without column names
            pattern2 = r'INSERT INTO (\w+) VALUES \((.+)\)'
            match = re.match(pattern2, sql, re.IGNORECASE)
            if not match:
                raise ValueError(f"Invalid INSERT syntax: {sql}")
            table_name = match.group(1)
            values_str = match.group(2)
            columns = None
        else:
            table_name = match.group(1)
            columns_str = match.group(2)
            values_str = match.group(3)
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse values
        values = SQLParser._parse_values(values_str)
        
        if columns:
            # Map values to columns
            if len(columns) != len(values):
                raise ValueError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")
            values_dict = dict(zip(columns, values))
            is_positional = False
        else:
            # For positional INSERT (no column names), store values as a list
            values_dict = {'__positional_values': values}
            is_positional = True
        
        return {
            'type': 'INSERT',
            'table_name': table_name,
            'values': values_dict,
            'is_positional': is_positional
        }
    
    @staticmethod
    def _parse_select(sql):
        # Replace newlines and multiple spaces
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # Parse SELECT statement
        # Pattern: SELECT columns FROM table [JOIN ...] [WHERE ...] [ORDER BY ...] [LIMIT ...]
        
        # Extract SELECT clause
        select_match = re.match(r'SELECT (.+?) FROM (.+)', sql, re.IGNORECASE)
        if not select_match:
            raise ValueError(f"Invalid SELECT syntax: {sql}")
        
        columns = select_match.group(1).strip()
        from_part = select_match.group(2).strip()
        
        # Parse FROM clause which may include JOINs
        # Find the main table (first word before JOIN, WHERE, ORDER BY, or LIMIT)
        main_table_match = re.match(r'(\w+)(?:\s+.*)?', from_part)
        if not main_table_match:
            raise ValueError(f"Could not parse table name from: {from_part}")
        
        table_name = main_table_match.group(1)
        
        # Initialize components
        join = None
        where = None
        order_by = None
        limit = None
        
        # Extract JOIN clause (everything from JOIN to WHERE/ORDER BY/LIMIT/end)
        join_pattern = r'(JOIN .+?)(?=(?: WHERE | ORDER BY | LIMIT |$))'
        join_match = re.search(join_pattern, from_part, re.IGNORECASE | re.DOTALL)
        if join_match:
            join = join_match.group(1).strip()
        
        # Extract WHERE clause
        where_pattern = r'WHERE (.+?)(?=(?: ORDER BY | LIMIT |$))'
        where_match = re.search(where_pattern, from_part, re.IGNORECASE)
        if where_match:
            where = where_match.group(1).strip()
        
        # Extract ORDER BY clause
        order_pattern = r'ORDER BY (.+?)(?=(?: LIMIT |$))'
        order_match = re.search(order_pattern, from_part, re.IGNORECASE)
        if order_match:
            order_by = order_match.group(1).strip()
        
        # Extract LIMIT clause
        limit_pattern = r'LIMIT (.+?)$'
        limit_match = re.search(limit_pattern, from_part, re.IGNORECASE)
        if limit_match:
            limit = limit_match.group(1).strip()
        
        return {
            'type': 'SELECT',
            'columns': columns,
            'table_name': table_name,
            'where': where,
            'join': join,
            'order_by': order_by,
            'limit': limit
        }
    
    @staticmethod
    def _parse_update(sql):
        # UPDATE table SET col1='val1', col2='val2' WHERE condition
        pattern = r'UPDATE (\w+) SET (.+?)(?: WHERE (.+))?$'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UPDATE syntax: {sql}")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where = match.group(3) if match.group(3) else None
        
        # Parse SET clause
        set_values = {}
        for pair in set_clause.split(','):
            col, value = pair.split('=', 1)
            col = col.strip()
            value = value.strip().strip("'")
            set_values[col] = value
        
        return {
            'type': 'UPDATE',
            'table_name': table_name,
            'set_values': set_values,
            'where': where
        }
    
    @staticmethod
    def _parse_delete(sql):
        # DELETE FROM table WHERE condition
        pattern = r'DELETE FROM (\w+)(?: WHERE (.+))?$'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid DELETE syntax: {sql}")
        
        table_name = match.group(1)
        where = match.group(2) if match.group(2) else None
        
        return {
            'type': 'DELETE',
            'table_name': table_name,
            'where': where
        }
    
    @staticmethod
    def _parse_create_index(sql):
        # CREATE INDEX index_name ON table_name(column_name)
        pattern = r'CREATE INDEX (\w+) ON (\w+)\((\w+)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid CREATE INDEX syntax: {sql}")
        
        index_name = match.group(1)
        table_name = match.group(2)
        column_name = match.group(3)
        
        return {
            'type': 'CREATE_INDEX',
            'index_name': index_name,
            'table_name': table_name,
            'column_name': column_name
        }
    
    @staticmethod
    def _parse_values(values_str):
        # Parse comma-separated values, handling quotes
        values = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in values_str:
            if char in ("'", '"') and (not in_quotes or char == quote_char):
                in_quotes = not in_quotes
                quote_char = char if in_quotes else None
                current += char
            elif char == ',' and not in_quotes:
                values.append(current.strip())
                current = ""
            else:
                current += char
        
        if current:
            values.append(current.strip())
        
        # Clean up values
        cleaned = []
        for val in values:
            val = val.strip()
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            elif val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            cleaned.append(val)
        
        return cleaned