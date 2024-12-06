import unittest
import os
import sys
import json
import tempfile
import logging
from typing import List, Dict, Any

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_code.driver import LogProcessingDriver
from base_code.engine import DistributedProcessingEngine
from base_code.validator import LogValidator

class TestLogProcessingSystem(unittest.TestCase):
    def setUp(self):
        """Setup test environment before each test method"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(os.path.dirname(__file__), '..', 'base_code', 'config.yaml')
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def create_test_log_file(self, filename: str, content: List[Dict[str, Any]]):
        """Create a test log file with given content
        Args:
            filename (str): Name of the log file
            content (List[Dict]): Log entries to write
        Returns:
            str: Full path to created log file"""
        file_path = os.path.join(self.temp_dir, filename)
        
        with open(file_path, 'w') as f:
            json.dump(content, f)
        
        return file_path

    def test_log_validation(self):
        """Test log file validation functionality"""
        validator = LogValidator()
        
        # Create a valid log file
        valid_log_entries = [
            {
                'timestamp': '2024-01-15T10:30:00Z',
                'log_level': 'INFO',
                'message': 'Test log entry',
                'source': 'test_system'
            }
        ]
        valid_log_file = self.create_test_log_file('valid_log.json', valid_log_entries)
        
        # Validate log file
        is_valid, errors = validator.validate_log_file(valid_log_file)
        
        self.assertTrue(is_valid, f"Valid log file should pass validation. Errors: {errors}")
    
    def test_driver_initialization(self):
        """Test log processing driver initialization"""
        driver = LogProcessingDriver(self.config_path)
        
        self.assertIsNotNone(driver.config, "Configuration should be loaded")
        self.assertIn('system', driver.config, "Configuration should have system settings")
    
    def test_distributed_processing(self):
        """Test distributed processing engine basic functionality"""
        engine = DistributedProcessingEngine(self.config_path)
        
        test_task = {
            'task_id': 'test_task_001',
            'log_file': 'sample.log'
        }
        
        result = engine._process_task(test_task)
        
        self.assertEqual(result['status'], 'success', "Task processing should succeed")
        self.assertIn('task_id', result, "Result should contain task ID")

def main():
    """Run test suite and generate report"""
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(unittest.makeSuite(TestLogProcessingSystem))
    
    # Generate JSON report
    report = {
        'total_tests': result.testsRun,
        'errors': len(result.errors),
        'failures': len(result.failures),
        'was_successful': result.wasSuccessful()
    }
    
    with open('test_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    sys.exit(not result.wasSuccessful())

if __name__ == '__main__':
    main()