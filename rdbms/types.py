class DataType:
    INT = 'INT'
    VARCHAR = 'VARCHAR'
    TEXT = 'TEXT'
    DATE = 'DATE'
    FLOAT = 'FLOAT'
    BOOL = 'BOOL'
    
    @staticmethod
    def validate(value, data_type, max_length=None):
        if value is None:
            return None
            
        # Handle VARCHAR with length specification
        if isinstance(data_type, str) and '(' in data_type and 'VARCHAR' in data_type.upper():
            # Extract just 'VARCHAR' for comparison
            data_type = 'VARCHAR'
        
        # Convert to uppercase for comparison
        data_type_upper = data_type.upper() if isinstance(data_type, str) else str(data_type).upper()
        
        if data_type_upper == DataType.INT:
            try:
                return int(value)
            except:
                raise ValueError(f"Invalid INT value: {value}")
                
        elif data_type_upper == DataType.FLOAT:
            try:
                return float(value)
            except:
                raise ValueError(f"Invalid FLOAT value: {value}")
                
        elif data_type_upper == DataType.VARCHAR:
            str_value = str(value)
            if max_length and len(str_value) > max_length:
                raise ValueError(f"VARCHAR exceeds max length {max_length}")
            return str_value
            
        elif data_type_upper == DataType.TEXT:
            return str(value)
            
        elif data_type_upper == DataType.BOOL:
            if isinstance(value, bool):
                return value
            val_str = str(value).lower()
            if val_str in ('true', '1', 't', 'yes'):
                return True
            if val_str in ('false', '0', 'f', 'no'):
                return False
            raise ValueError(f"Invalid BOOL value: {value}")
            
        elif data_type_upper == DataType.DATE:
            # Simple date validation
            if isinstance(value, str) and len(value) == 10:
                return value  # YYYY-MM-DD format
            raise ValueError(f"Invalid DATE format: {value}")
            
        else:
            raise ValueError(f"Unknown data type: {data_type}")