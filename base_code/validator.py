import os
import csv
import json
import logging
from typing import List, Dict, Any, Tuple

class LogValidator:
    def __init__(self, required_columns: List[str] = None):
        """Initialize log validator with optional required columns
        Args:
            required_columns (List[str]): Mandatory columns for log validation"""
        self.logger = logging.getLogger(__name__)
        self.required_columns = required_columns or [
            'timestamp', 'log_level', 'message', 'source'
        ]

    def validate_log_file(self, file_path: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate a log file's structure and content
        Args:
            file_path (str): Path to the log file
        Returns:
            Tuple of (validation_status, error_details)"""
        # Determine file type
        file_extension = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_extension == '.csv':
                return self._validate_csv(file_path)
            elif file_extension == '.json':
                return self._validate_json(file_path)
            elif file_extension in ['.log', '.txt']:
                return self._validate_text(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
        
        except Exception as e:
            self.logger.error(f"Validation error for {file_path}: {e}")
            return False, [{'error': str(e)}]

    def _validate_csv(self, file_path: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate CSV log files
        Args:
            file_path (str): Path to CSV file
        Returns:
            Validation result and error details"""
        errors = []
        
        with open(file_path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            
            # Check for required columns
            missing_columns = set(self.required_columns) - set(csv_reader.fieldnames)
            if missing_columns:
                errors.append({
                    'error_type': 'missing_columns',
                    'missing_columns': list(missing_columns)
                })
            
            # Track unique rows to detect duplicates
            seen_rows = set()
            
            for row_index, row in enumerate(csv_reader, 1):
                row_hash = hash(tuple(row.values()))
                
                # Check for duplicate rows
                if row_hash in seen_rows:
                    errors.append({
                        'error_type': 'duplicate_row',
                        'row_index': row_index
                    })
                else:
                    seen_rows.add(row_hash)
                
                # Perform row-level validations
                for column in self.required_columns:
                    if not row.get(column):
                        errors.append({
                            'error_type': 'missing_field',
                            'column': column,
                            'row_index': row_index
                        })
        
        return len(errors) == 0, errors

    def _validate_json(self, file_path: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate JSON log files
        Args:
            file_path (str): Path to JSON file
        Returns:
            Validation result and error details"""
        errors = []
        
        try:
            with open(file_path, 'r') as jsonfile:
                log_data = json.load(jsonfile)
                
                # Support both single object and list of objects
                if not isinstance(log_data, list):
                    log_data = [log_data]
                
                for index, entry in enumerate(log_data, 1):
                    for column in self.required_columns:
                        if column not in entry or not entry[column]:
                            errors.append({
                                'error_type': 'missing_field',
                                'column': column,
                                'entry_index': index
                            })
        
        except json.JSONDecodeError as e:
            errors.append({
                'error_type': 'json_decode_error',
                'message': str(e)
            })
        
        return len(errors) == 0, errors

    def _validate_text(self, file_path: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """Basic validation for text/log files
        Args:
            file_path (str): Path to text file
        Returns:
            Validation result and error details"""
        errors = []
    
        try:
            with open(file_path, 'r') as textfile:
                log_lines = textfile.readlines()
            
                # Check for empty file
                if not log_lines:
                    errors.append({
                        'error_type': 'empty_file'
                    })
            
                # Basic format checking (assuming timestamp, log level, message)
                for line_num, line in enumerate(log_lines, 1):
                    line = line.strip()

                    # Skip empty lines
                    if not line:
                        continue
                
                    # Split log line by pipe delimiter
                    parts = line.split('|')
                
                    # Validate line structure
                    if len(parts) < 3:
                        errors.append({
                            'error_type': 'invalid_line_format',
                            'line_number': line_num,
                            'line_content': line,
                            'expected_parts': 3,
                            'actual_parts': len(parts)
                        })
                        continue
                
                    # Validate timestamp
                    timestamp = parts[0].strip()
                    if not self.validate_log_timestamp(timestamp):
                        errors.append({
                            'error_type': 'invalid_timestamp',
                            'line_number': line_num,
                            'timestamp': timestamp
                        })
                
                    # Validate log level
                    log_level = parts[1].strip()
                    valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
                    if log_level not in valid_log_levels:
                        errors.append({
                            'error_type': 'invalid_log_level',
                            'line_number': line_num,
                            'log_level': log_level,
                            'valid_levels': valid_log_levels
                        })
                
                    # Validate message content
                    message = parts[2].strip()
                    if not message:
                        errors.append({
                            'error_type': 'empty_message',
                            'line_number': line_num
                        })
    
        except IOError as io_error:
            errors.append({
                'error_type': 'file_read_error',
                'error_message': str(io_error),
                'file_path': file_path
            })
    
        except Exception as e:
            errors.append({
                'error_type': 'unexpected_validation_error',
                'error_message': str(e),
                'file_path': file_path
            })
    
        return len(errors) == 0, errors
    
    def generate_validation_report(self, file_path: str) -> Dict[str, Any]:
        """Generate comprehensive validation report
        Args:
            file_path (str): Path to log file
        Returns:
            Detailed validation report"""
        validation_status, errors = self.validate_log_file(file_path)
        
        return {
            'file_path': file_path,
            'validation_status': validation_status,
            'total_errors': len(errors),
            'error_details': errors,
            'file_size': os.path.getsize(file_path),
            'validation_timestamp': datetime.now().isoformat()
        }

def configure_logging():
    """Configure logging for the validator
    Returns:
        logging.Logger: Configured logger"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('log_validation.log')
        ]
    )
    return logging.getLogger(__name__)

def main():
    """Main execution for log file validation"""
    logger = configure_logging()
    validator = LogValidator()
    
    # Example log files directory
    log_directory = './logs'
    
    for filename in os.listdir(log_directory):
        file_path = os.path.join(log_directory, filename)
        
        if os.path.isfile(file_path):
            try:
                report = validator.generate_validation_report(file_path)
                logger.info(f"Validation Report for {filename}: {report}")
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")

if __name__ == '__main__':
    main()