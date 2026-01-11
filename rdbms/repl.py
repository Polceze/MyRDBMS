"""Read-Eval-Print Loop (REPL) for SQL interface"""

import sys
from .executor import QueryExecutor

class REPL:
    # initialize REPL    
    def __init__(self):
        self.executor = QueryExecutor()
        self.running = False
    
    def start(self):
        """Start the REPL"""
        print("=" * 60)
        print("SimpleRDBMS v1.0 - Type 'exit' to quit, 'help' for commands")
        print("=" * 60)
        
        self.running = True
        while self.running:
            try:
                # Multi-line input support
                sql = input("\nsql> ")
                
                # Check for exit
                if sql.lower() == 'exit':
                    self.running = False
                    print("Goodbye!")
                    continue
                
                # Check for help
                if sql.lower() == 'help':
                    self._show_help()
                    continue
                
                # Check for multi-line input (ends with ;)
                while not sql.strip().endswith(';'):
                    line = input("... ")
                    sql += " " + line
                
                # Remove trailing semicolon
                sql = sql.strip()[:-1]
                
                # Execute query
                result = self.executor.execute_raw(sql)
                
                # Display result
                self._display_result(result, sql)
                
            except KeyboardInterrupt:
                print("\nInterrupted. Type 'exit' to quit.")
            except Exception as e:
                print(f"Error: {e}")
    
    def _show_help(self):
        """Display help information"""
        print("\nAvailable commands:")
        print("  CREATE TABLE table_name (col1 TYPE, col2 TYPE, PRIMARY KEY(col))")
        print("  INSERT INTO table_name VALUES ('val1', 'val2')")
        print("  SELECT * FROM table_name [WHERE condition] [JOIN ...]")
        print("  UPDATE table_name SET col='value' WHERE condition")
        print("  DELETE FROM table_name WHERE condition")
        print("  CREATE INDEX index_name ON table_name(column_name)")
        print("\nData types: INT, VARCHAR(n), TEXT, DATE, FLOAT, BOOL")
        print("\nExamples:")
        print("  CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50))")
        print("  INSERT INTO students VALUES (1, 'John Doe')")
        print("  SELECT * FROM students WHERE name = 'John Doe'")
        print("  SELECT * FROM students INNER JOIN grades ON students.id = grades.student_id")
    
    def _display_result(self, result, sql):
        """Display query results"""
        sql_upper = sql.upper()
        
        if sql_upper.startswith('SELECT'):
            if isinstance(result, list):
                if not result:
                    print("No rows found")
                else:
                    # Get column names
                    columns = list(result[0].keys())
                    
                    # Print header
                    print("\n" + " | ".join(columns))
                    print("-" * (len(" | ".join(columns))))
                    
                    # Print rows
                    for row in result:
                        values = []
                        for col in columns:
                            val = row.get(col, 'NULL')
                            values.append(str(val))
                        print(" | ".join(values))
                    
                    print(f"\n{len(result)} row(s) returned")
            else:
                print(f"Result: {result}")
        
        elif sql_upper.startswith('INSERT'):
            print(f"Inserted row with ID: {result}")
        
        elif sql_upper.startswith('UPDATE'):
            print(f"Updated {result} row(s)")
        
        elif sql_upper.startswith('DELETE'):
            print(f"Deleted {result} row(s)")
        
        elif sql_upper.startswith('CREATE'):
            print("Command executed successfully")
        
        else:
            print(f"Result: {result}")