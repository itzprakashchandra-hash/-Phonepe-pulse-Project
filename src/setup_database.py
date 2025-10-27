import os
import json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import requests
import zipfile
from pathlib import Path

class PhonePeDataSetup:
    def __init__(self, mysql_config):
        """Initialize with MySQL configuration"""
        self.mysql_config = mysql_config
        self.data_dir = Path("Data")
        self.data_dir.mkdir(exist_ok=True)
        
    def download_phonepe_data(self):
        """Download PhonePe Pulse data from GitHub"""
        print("📥 Downloading PhonePe Pulse data...")
        
        # GitHub repository URL
        repo_url = "https://github.com/PhonePe/pulse/archive/refs/heads/master.zip"
        zip_path = self.data_dir / "phonepe_data.zip"
        
        try:
            # Download the zip file
            response = requests.get(repo_url, stream=True)
            response.raise_for_status()
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print("✅ Download complete!")
            
            # Extract the zip file
            print("📦 Extracting data...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.data_dir)
            
            print("✅ Extraction complete!")
            
            # Remove zip file
            zip_path.unlink()
            
            return True
        except Exception as e:
            print(f"❌ Error downloading data: {e}")
            return False
    
    def create_database(self):
        """Create PhonePe database if it doesn't exist"""
        print("\n🗄️ Creating database...")
        
        try:
            # Connect without specifying database
            conn = mysql.connector.connect(
                host=self.mysql_config['host'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password']
            )
            cursor = conn.cursor()
            
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']}")
            print(f"✅ Database '{self.mysql_config['database']}' created/verified!")
            
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            print(f"❌ Error creating database: {e}")
            return False
    
    def create_tables(self):
        """Create necessary tables"""
        print("\n📋 Creating tables...")
        
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.mysql_config['user']}:{quote_plus(self.mysql_config['password'])}@"
                f"{self.mysql_config['host']}/{self.mysql_config['database']}"
            )
            
            with engine.connect() as conn:
                # Aggregated Transaction Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS aggregated_transaction (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    Transaction_type VARCHAR(100),
                    Transaction_count BIGINT,
                    Transaction_amount DOUBLE,
                    PRIMARY KEY (State, Year, Quarter, Transaction_type)
                )
                """))
                
                # Aggregated User Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS aggregated_user (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    Brands VARCHAR(100),
                    Transaction_count BIGINT,
                    Percentage DOUBLE,
                    PRIMARY KEY (State, Year, Quarter, Brands)
                )
                """))
                
                # Map Transaction Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS map_transaction (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    District VARCHAR(100),
                    Transaction_count BIGINT,
                    Transaction_amount DOUBLE,
                    PRIMARY KEY (State, Year, Quarter, District)
                )
                """))
                
                # Map User Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS map_user (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    District VARCHAR(100),
                    RegisteredUsers BIGINT,
                    AppOpens BIGINT,
                    PRIMARY KEY (State, Year, Quarter, District)
                )
                """))
                
                # Top Transaction Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS top_transaction (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    Pincode INT,
                    Transaction_count BIGINT,
                    Transaction_amount DOUBLE,
                    PRIMARY KEY (State, Year, Quarter, Pincode)
                )
                """))
                
                # Top User Table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS top_user (
                    State VARCHAR(100),
                    Year INT,
                    Quarter INT,
                    Pincode INT,
                    RegisteredUsers BIGINT,
                    PRIMARY KEY (State, Year, Quarter, Pincode)
                )
                """))
                
                conn.commit()
                
            print("✅ All tables created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            return False 
    def extract_aggregated_transaction(self):
        """Extract aggregated transaction data from JSON files"""
        print("\n📊 Processing aggregated transaction data...")
        
        data_list = []
        data_path = self.data_dir / "pulse-master/data/aggregated/transaction/country/india/state"
        
        try:
            for state_folder in data_path.iterdir():
                if state_folder.is_dir():
                    state = state_folder.name
                    
                    for year_folder in state_folder.iterdir():
                        if year_folder.is_dir():
                            year = year_folder.name
                            
                            for json_file in year_folder.glob("*.json"):
                                quarter = json_file.stem
                                
                                with open(json_file, 'r') as f:
                                    data = json.load(f)
                                    
                                    for transaction in data['data']['transactionData']:
                                        data_list.append({
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(quarter),
                                            'Transaction_type': transaction['name'],
                                            'Transaction_count': transaction['paymentInstruments'][0]['count'],
                                            'Transaction_amount': transaction['paymentInstruments'][0]['amount']
                                        })
            
            df = pd.DataFrame(data_list)
            print(f"✅ Extracted {len(df)} transaction records")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting transaction data: {e}")
            return pd.DataFrame()
    
    def extract_aggregated_user(self):
        """Extract aggregated user data from JSON files"""
        print("\n👥 Processing aggregated user data...")
        
        data_list = []
        data_path = self.data_dir / "pulse-master/data/aggregated/user/country/india/state"
        
        try:
            for state_folder in data_path.iterdir():
                if state_folder.is_dir():
                    state = state_folder.name
                    
                    for year_folder in state_folder.iterdir():
                        if year_folder.is_dir():
                            year = year_folder.name
                            
                            for json_file in year_folder.glob("*.json"):
                                quarter = json_file.stem
                                
                                with open(json_file, 'r') as f:
                                    data = json.load(f)
                                    
                                    if data['data']['usersByDevice']:
                                        for device in data['data']['usersByDevice']:
                                            data_list.append({
                                                'State': state.replace('-', ' ').title(),
                                                'Year': int(year),
                                                'Quarter': int(quarter),
                                                'Brands': device['brand'],
                                                'Transaction_count': device['count'],
                                                'Percentage': device['percentage']
                                            })
            
            df = pd.DataFrame(data_list)
            print(f"✅ Extracted {len(df)} user records")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting user data: {e}")
            return pd.DataFrame()
    
    def load_data_to_mysql(self, df, table_name):
        """Load DataFrame to MySQL table"""
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.mysql_config['user']}:{quote_plus(self.mysql_config['password'])}@"
                f"{self.mysql_config['host']}/{self.mysql_config['database']}"
            )
            
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"✅ Loaded {len(df)} records into {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading data to {table_name}: {e}")
            return False
    
    def run_full_setup(self):
        """Run complete setup process"""
        print("="*60)
        print("PhonePe Pulse Data Setup")
        print("="*60)
        
        # Step 1: Download data
        if not self.download_phonepe_data():
            return False
        
        # Step 2: Create database
        if not self.create_database():
            return False
        
        # Step 3: Create tables
        if not self.create_tables():
            return False
        
        # Step 4: Extract and load transaction data
        df_transaction = self.extract_aggregated_transaction()
        if not df_transaction.empty:
            self.load_data_to_mysql(df_transaction, 'aggregated_transaction')
        
        # Step 5: Extract and load user data
        df_user = self.extract_aggregated_user()
        if not df_user.empty:
            self.load_data_to_mysql(df_user, 'aggregated_user')
        
        print("\n" + "="*60)
        print("✅ Setup Complete!")
        print("="*60)
        
        return True


if __name__ == "__main__":
    # MySQL Configuration
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'sql@123',  # Your MySQL password
        'database': 'phonepe_pulse'
    }
    
    # Run setup
    setup = PhonePeDataSetup(mysql_config)
    setup.run_full_setup()