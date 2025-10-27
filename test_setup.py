import sys
import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
import plotly
import streamlit as st
import sqlalchemy
import pymysql

def test_imports():
    """Test if all required packages are installed"""
    print("Testing package imports...")
    print("✓ pandas version:", pd.__version__)
    print("✓ numpy version:", np.__version__)
    print("✓ matplotlib version:", matplotlib.__version__)
    print("✓ seaborn version:", sns.__version__)
    print("✓ plotly version:", plotly.__version__)
    print("✓ streamlit version:", st.__version__)
    print("✓ sqlalchemy version:", sqlalchemy.__version__)
    print("✓ pymysql version:", pymysql.__version__)
    print("\n✅ All packages imported successfully!")

def test_python_version():
    """Check Python version"""
    print(f"\nPython version: {sys.version}")
    if sys.version_info >= (3, 8):
        print("✅ Python version is compatible")
    else:
        print("⚠️ Python 3.8+ is recommended")

def test_folders():
    """Check if required folders exist"""
    import os
    print("\nChecking project structure...")
    folders = ['Data', 'Sql_scripts', 'src']
    for folder in folders:
        if os.path.exists(folder):
            print(f"✓ {folder}/ exists")
        else:
            print(f"✗ {folder}/ NOT FOUND")

if __name__ == "__main__":
    print("="*50)
    print("PhonePe Project Setup Verification")
    print("="*50)
    
    test_python_version()
    test_imports()
    test_folders()
    
    print("\n" + "="*50)
    print("Setup verification complete!")
    print("="*50)