# Parallel Duplicate Finder

A high-performance, memory-efficient Python script for finding duplicate lines in large text files. The script combines the best features of parallel processing, adaptive memory management, and robust duplicate detection to handle datasets of any size.

## Features

- **Three Processing Strategies**: Automatically selects the optimal strategy based on available memory and data size
- **Parallel Processing**: Utilizes multiple CPU cores for maximum performance
- **Memory Efficient**: Adaptive strategies to handle datasets from MB to hundreds of GB
- **Robust Hash Detection**: Uses xxhash (if available) or Blake2b for fast and reliable duplicate detection
- **Comprehensive Logging**: Detailed logging with timestamps and progress tracking
- **Cross-platform**: Works on Windows, macOS, and Linux

## Processing Strategies

### 1. Fast Mode (Default for small datasets)
- **Best for**: Datasets that fit comfortably in memory
- **Memory usage**: ~40% of total file size
- **Speed**: Fastest processing time
- **Method**: Loads all data into memory for immediate processing

### 2. Safe Mode (Default for medium datasets)
- **Best for**: Large datasets with limited memory
- **Memory usage**: ~10% of total file size
- **Speed**: Moderate processing time
- **Method**: Two-pass processing - first pass counts hashes, second pass extracts duplicates

### 3. Disk Mode (Default for huge datasets)
- **Best for**: Extremely large datasets (hundreds of GB)
- **Memory usage**: Minimal, independent of file size
- **Speed**: Slower but handles unlimited data size
- **Method**: External sorting with temporary files and merge operations

## Installation

### Prerequisites
- Python 3.7 or higher
- pip package manager

### Required Dependencies
```bash
pip install psutil xxhash tqdm
```

Or install from requirements.txt:
```bash
pip install -r requirements.txt
```

### Optional Dependencies
- **psutil**: For automatic memory detection and strategy selection
- **xxhash**: For faster hash computation (falls back to hashlib if not available)

## Usage

### Basic Usage
```bash
python duplicates.py
```
This will automatically select the best strategy and process all CSV files in the `input/` directory.

### Command Line Options

```bash
python duplicates.py [options]
```

#### Available Options:

- `-i, --input`: Input directory (default: `input/`)
- `-s, --strategy`: Processing strategy (`auto`, `fast`, `safe`, `disk`) (default: `auto`)
- `-wl, --write-length`: Length of duplicate line prefix to write (default: 47)
- `-hf, --hash-fields`: Number of fields to use for hashing (default: 6)
- `-hd, --hash-delimiter`: Field delimiter character (default: `;`)
- `-fp, --file-pattern`: File pattern for filtering (default: `*.csv`)

### Examples

#### Automatic mode (recommended):
```bash
python duplicates.py
```

#### Force fast mode:
```bash
python duplicates.py --strategy fast
```

#### Process specific directory with custom delimiter:
```bash
python duplicates.py -i "C:\data" --hash-delimiter "|"
```

#### Process only files matching pattern:
```bash
python duplicates.py --file-pattern "*_2024_*.csv"
```

#### Custom hash field count:
```bash
python duplicates.py --hash-fields 4
```

## How It Works

1. **File Discovery**: Scans the input directory for files matching the specified pattern
2. **Strategy Selection**: Automatically chooses the optimal processing strategy based on:
   - Available system memory
   - Total size of input files
   - Memory usage thresholds
3. **Parallel Processing**: Distributes work across multiple CPU cores
4. **Duplicate Detection**: Uses configurable field-based hashing to identify duplicates
5. **Result Output**: Writes found duplicates to `duplicates.txt` with source file information

## Output Format

The script generates a `duplicates.txt` file containing:
- Duplicate line prefixes
- Source file names where each duplicate was found
- Indication of within-file vs. cross-file duplicates

Example output:
```
John;Doe;1985-01-01;Manager;Sales;New York
    - employees_2023.csv
    - employees_2024.csv

Jane;Smith;1990-05-15;Developer;IT;Boston
    - (Within-file duplicates) employees_2024.csv
```

## Performance Considerations

- **CPU Cores**: Uses (CPU cores - 1) workers by default for optimal performance
- **Memory Usage**: Automatically adjusts strategy based on available RAM
- **Disk Space**: Disk mode requires temporary space approximately equal to input size
- **Hash Algorithm**: xxhash provides 2-3x faster hashing than standard library

## Configuration

The script uses several internal constants that can be modified:

- `MAX_WORKERS`: Maximum number of parallel processes
- `RAM_USAGE_THRESHOLD`: Maximum RAM usage percentage (default: 70%)
- `DISK_CHUNK_SIZE_MB`: Chunk size for disk mode processing (default: 128MB)

## Logging

The script creates detailed logs in the `logs/` directory with:
- Timestamp information
- Processing progress
- Memory usage statistics
- Error handling details
- Performance metrics

## Troubleshooting

### Common Issues

1. **Out of Memory**: Use `--strategy safe` or `--strategy disk`
2. **Slow Performance**: Ensure xxhash is installed: `pip install xxhash`
3. **Permission Errors**: Check read/write permissions for input and output directories
4. **Unicode Issues**: The script handles UTF-8 encoding and ignores invalid characters

### System Requirements

- **Minimum RAM**: 1GB for small datasets
- **Recommended RAM**: 8GB+ for optimal performance
- **Disk Space**: 2x input size for disk mode temporary files
- **CPU**: Multi-core processor recommended for parallel processing

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.