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
            
        if data_type == DataType.INT:
            try:
                return int(value)
            except:
                raise ValueError(f"Invalid INT value: {value}")
                
        elif data_type == DataType.FLOAT:
            try:
                return float(value)
            except:
                raise ValueError(f"Invalid FLOAT value: {value}")
                
        elif data_type == DataType.VARCHAR:
            if max_length and len(str(value)) > max_length:
                raise ValueError(f"VARCHAR exceeds max length {max_length}")
            return str(value)
            
        elif data_type == DataType.TEXT:
            return str(value)
            
        elif data_type == DataType.BOOL:
            if isinstance(value, bool):
                return value
            if str(value).lower() in ('true', '1', 't', 'yes'):
                return True
            if str(value).lower() in ('false', '0', 'f', 'no'):
                return False
            raise ValueError(f"Invalid BOOL value: {value}")
            
        elif data_type == DataType.DATE:
            # Date validation
            if isinstance(value, str) and len(value) == 10:
                return value  # YYYY-MM-DD format
            raise ValueError(f"Invalid DATE format: {value}")
            
        else:
            raise ValueError(f"Unknown data type: {data_type}")