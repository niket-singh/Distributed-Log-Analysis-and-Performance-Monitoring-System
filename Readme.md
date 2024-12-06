# Distributed Log Analysis and Performance Monitoring System

## Project Overview
This is a scalable log analysis system designed to process large-scale log files across distributed computing environments. The project demonstrates advanced parallel processing, data validation, and performance monitoring techniques.

## System Requirements
- Python 3.8+
- Operating Systems: Linux, macOS, Windows
- Minimum 8 GB RAM
- Multi-core processor recommended

## Prerequisites

### 1. Python Environment Setup
```bash
# Create a virtual environment
python3 -m venv log_analyzer_env

# Activate the virtual environment
# On Windows
log_analyzer_env\Scripts\activate
# On macOS/Linux
source log_analyzer_env/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

### 2. Configuration
Modify `config.yaml` to customize system settings:
- Adjust `max_workers` for parallel processing
- Configure network settings
- Set logging preferences

## Running the System

### 1. Log Processing
```bash
# Run the main driver script
python driver.py

# Optional: Specify log directory
python driver.py --log-dir /path/to/logs
```

### 2. Start Distributed Processing Engine
```bash
# Launch the network processing server
python engine.py
```

### 3. Validate Log Files
```bash
# Run log file validation
python validator.py --directory ./logs
```

## Testing the System
```bash
# Execute comprehensive test suite
python test_runner.py
```

## Monitoring and Logging
- System logs are stored in `system.log`
- Performance metrics are logged in `performance.log`

## Troubleshooting
- Ensure all dependencies are installed
- Check network configurations
- Verify log file formats match supported types
- Review `system.log` for detailed error information

## Performance Optimization Tips
- Increase `max_workers` for larger datasets
- Use SSD for faster I/O operations
- Monitor system resources during processing

## Security Considerations
- Validate and sanitize input log files
- Use secure network configurations
- Implement access controls for distributed processing
