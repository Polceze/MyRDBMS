import re

# Parser for SQL commands
class SQLParser:
    
    @staticmethod
    def parse(sql):
        """Parse a SQL command and return its components"""
        sql = sql.strip().upper()
        
        if sql.startswith('CREATE TABLE'):
            return SQLParser._parse_create_table(sql)
        elif sql.startswith('INSERT INTO'):
            return SQLParser._parse_insert(sql)
        elif sql.startswith('SELECT'):
            return SQLParser._parse_select(sql)
        elif sql.startswith('UPDATE'):
            return SQLParser._parse_update(sql)
        elif sql.startswith('DELETE FROM'):
            return SQLParser._parse_delete(sql)
        elif sql.startswith('CREATE INDEX'):
            return SQLParser._parse_create_index(sql)
        else:
            raise ValueError(f"Unsupported SQL command: {sql}")
    
    @staticmethod
    def _parse_create_table(sql):
        # parse CREATE TABLE table_name (col1 TYPE, col2 TYPE, ..., PRIMARY KEY(col), UNIQUE(col))
        pattern = r'CREATE TABLE (\w+) \((.+)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid CREATE TABLE syntax: {sql}")
        
        table_name = match.group(1)
        columns_str = match.group(2).strip()
        
        # Parse columns and constraints
        columns = []
        primary_key = None
        unique_keys = []
        
        # Split by comma, but handle nested parentheses
        tokens = []
        current = ""
        paren_depth = 0
        
        for char in columns_str:
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and paren_depth == 0:
                tokens.append(current.strip())
                current = ""
            else:
                current += char
        
        if current:
            tokens.append(current.strip())
        
        for token in tokens:
            token_upper = token.upper()
            
            if token_upper.startswith('PRIMARY KEY'):
                # PRIMARY KEY(col_name)
                pk_match = re.search(r'PRIMARY KEY\((\w+)\)', token, re.IGNORECASE)
                if pk_match:
                    primary_key = pk_match.group(1)
                continue
            
            if token_upper.startswith('UNIQUE'):
                # UNIQUE(col_name)
                unique_match = re.search(r'UNIQUE\((\w+)\)', token, re.IGNORECASE)
                if unique_match:
                    unique_keys.append(unique_match.group(1))
                continue
            
            # Regular column: name TYPE [NOT NULL]
            col_parts = token.split()
            if len(col_parts) < 2:
                raise ValueError(f"Invalid column definition: {token}")
            
            col_name = col_parts[0]
            col_type = col_parts[1]
            
            # Handle VARCHAR(255) type
            if '(' in col_type:
                base_type = col_type.split('(')[0]
                if base_type.upper() == 'VARCHAR':
                    col_type = col_type  # Keep VARCHAR(255) as is
            
            nullable = True
            if len(col_parts) > 2 and ' '.join(col_parts[2:]).upper() == 'NOT NULL':
                nullable = False
            
            columns.append((col_name, col_type, 'NOT NULL' if not nullable else None))
        
        return {
            'type': 'CREATE_TABLE',
            'table_name': table_name,
            'columns': columns,
            'primary_key': primary_key,
            'unique_keys': unique_keys
        }
    
    @staticmethod
    def _parse_insert(sql):
        # INSERT INTO table_name (col1, col2) VALUES ('val1', 'val2')
        pattern = r'INSERT INTO (\w+) \((.+?)\) VALUES \((.+?)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            # Try without column names
            pattern2 = r'INSERT INTO (\w+) VALUES \((.+?)\)'
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
        else:
            values_dict = {f'col{i}': val for i, val in enumerate(values)}
        
        return {
            'type': 'INSERT',
            'table_name': table_name,
            'values': values_dict
        }
    
    @staticmethod
    def _parse_select(sql):
        # SELECT * FROM table [WHERE condition] [JOIN ...]
        pattern = r'SELECT (.+?) FROM (\w+)(?: WHERE (.+?))?(?: (JOIN.+))?$'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid SELECT syntax: {sql}")
        
        columns = match.group(1).strip()
        table_name = match.group(2)
        where = match.group(3) if match.group(3) else None
        join = match.group(4) if match.group(4) else None
        
        return {
            'type': 'SELECT',
            'columns': columns,
            'table_name': table_name,
            'where': where,
            'join': join
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