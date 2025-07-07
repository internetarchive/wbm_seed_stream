#!/usr/bin/env python3
"""
Test script to verify PySpark setup and environment
"""

# Suppress MKL warnings FIRST before any other imports
import warnings
warnings.filterwarnings('ignore')

import os
import sys

# Set MKL environment variables to suppress warnings
os.environ['MKL_DISABLE_FAST_MM'] = '1'
os.environ['MKL_THREADING_LAYER'] = 'GNU'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['MKL_VERBOSE'] = '0'
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

import subprocess

def test_spark_environment():
    """Test and configure Spark environment"""
    
    print("=== PySpark Environment Test ===")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    
    # Set environment variables
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # Test 1: Basic PySpark import
    try:
        from pyspark.sql import SparkSession
        print("✓ Successfully imported PySpark")
    except ImportError as e:
        print(f"✗ Failed to import PySpark: {e}")
        return False
    
    # Test 2: Create SparkSession
    try:
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .master("local[1]") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        print("✓ Successfully created SparkSession")
        
        # Test 3: Simple DataFrame operation
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = spark.createDataFrame(data, columns)
        
        print("✓ Successfully created DataFrame")
        print(f"DataFrame count: {df.count()}")
        
        # Test 4: Simple transformation
        result = df.filter(df.age > 28).collect()
        print(f"✓ Successfully applied filter, results: {len(result)} rows")
        
        spark.stop()
        print("✓ Successfully stopped SparkSession")
        return True
        
    except Exception as e:
        print(f"✗ Spark test failed: {e}")
        return False

def test_spark_submit():
    """Test spark-submit command availability"""
    try:
        result = subprocess.run(
            ["spark-submit", "--version"], 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        print("✓ spark-submit is available")
        print(f"Version info: {result.stderr.split('version')[1].split()[0] if 'version' in result.stderr else 'Unknown'}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"✗ spark-submit test failed: {e}")
        return False

def check_python_environment():
    """Check for potential issues in Python environment"""
    print("\n=== Python Environment Check ===")
    
    # Check for sitecustomize.py
    import site
    site_packages = site.getsitepackages()
    print(f"Site packages: {site_packages}")
    
    for sp in site_packages:
        sitecustomize_path = os.path.join(sp, "sitecustomize.py")
        if os.path.exists(sitecustomize_path):
            print(f"⚠ Found sitecustomize.py at: {sitecustomize_path}")
            with open(sitecustomize_path, 'r') as f:
                content = f.read()
                if 'print' in content:
                    print("⚠ sitecustomize.py contains print statements - this may cause issues")
    
    # Check PYTHONPATH
    pythonpath = os.environ.get('PYTHONPATH', '')
    if pythonpath:
        print(f"PYTHONPATH: {pythonpath}")
    else:
        print("PYTHONPATH not set")

if __name__ == "__main__":
    print("Starting comprehensive Spark environment test...")
    
    check_python_environment()
    
    spark_available = test_spark_environment()
    submit_available = test_spark_submit()
    
    if spark_available and submit_available:
        print("\n✓ All tests passed! Your Spark environment should work correctly.")
    else:
        print("\n✗ Some tests failed. Please address the issues above.")