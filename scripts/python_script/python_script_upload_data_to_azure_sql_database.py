import pandas as pd
import numpy as np
import os
import glob
import pyodbc
import time
import urllib.parse
from sqlalchemy import create_engine, text

def process_and_upload_excel_files():
    """Process Excel files and upload to Azure SQL DB"""
    print("=== VNPAY Excel to Azure SQL Database (Large File Version) ===")
    
    # Configuration
    excel_dir = r"D:\VNPAY\Master MC-20250420T081641Z-006\Master MC"
    server = 'hungnguyen.database.windows.net'
    database = 'HungNguyen'
    username = '***'
    password = '***'
    table_name = 'alltransactions_6'
    
    # Find Excel files
    excel_files = glob.glob(os.path.join(excel_dir, "*.xlsx"))
    if not excel_files:
        print(f"No Excel files found in {excel_dir}")
        return
    
    print(f"Found {len(excel_files)} Excel files")
    
    # Connect to Azure SQL
    try:
        # Create connection string
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=60;"  # Increased timeout
        )
        
        # Test connection
        print("Testing connection...")
        pyodbc.connect(conn_str).close()
        print("Connection successful!")
        
        # Create SQLAlchemy engine with optimized parameters
        params = urllib.parse.quote_plus(conn_str)
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}", 
            fast_executemany=True,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        # Ensure table exists - don't stop if this fails
        try:
            ensure_table_exists(engine)
        except Exception as schema_error:
            print(f"Warning: Error creating schema/table: {str(schema_error)}")
            print("Continuing anyway, assuming table structure exists...")
        
        # Process files
        total_rows = 0
        files_processed = 0
        
        for file_num, file_path in enumerate(excel_files, 1):
            file_name = os.path.basename(file_path)
            print(f"\nProcessing file {file_num}/{len(excel_files)}: {file_name}")
            
            try:
                # Load and process Excel file
                start_time = time.time()
                df = load_and_process_file(file_path)
                
                if df is None or len(df) == 0:
                    print("No valid data found in file, skipping")
                    continue
                
                # Upload to SQL
                upload_success = upload_to_sql(df, engine, table_name)
                
                # Update counters
                if upload_success:
                    file_rows = len(df)
                    total_rows += file_rows
                    files_processed += 1
                    
                    # Report time
                    elapsed = time.time() - start_time
                    elapsed_mins = int(elapsed // 60)
                    elapsed_secs = int(elapsed % 60)
                    print(f"✅ Processed {file_rows:,} rows in {elapsed_mins}m {elapsed_secs}s")
                
            except Exception as e:
                print(f"❌ Error processing file {file_name}: {str(e)}")
                # Continue with next file
        
        print(f"\n=== Summary ===")
        print(f"Total files found: {len(excel_files)}")
        print(f"Total files processed successfully: {files_processed}")
        print(f"Total rows uploaded: {total_rows:,}")
        
        if files_processed > 0:
            print("\nData has been successfully loaded into the Azure SQL database")
            print("Table: vnpay.alltransactions_6")
        else:
            print("\nNo files were successfully processed")
            
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")
        
        print("\nTroubleshooting tips:")
        print("1. Check your Azure SQL server and credentials")
        print("2. Verify your IP is allowed in Azure SQL firewall")
        print("3. Check if ODBC Driver 18 is installed")
        
    finally:
        # If we created an engine, close it
        if 'engine' in locals():
            engine.dispose()
        print("\nProcess completed.")

def ensure_table_exists(engine):
    """Create schema and table if they don't exist"""
    try:
        # First, check if the schema exists and create it if needed
        schema_check = """
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'vnpay')
        BEGIN
            EXEC('CREATE SCHEMA vnpay')
        END
        """
        
        # Then check if table exists and create it if needed
        table_check = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'vnpay.alltransactions_6') AND type in (N'U'))
        BEGIN
            CREATE TABLE vnpay.alltransactions_6 (
                [STT] INT,
                [Mã GD] VARCHAR(12),
                [Mã thanh toán] VARCHAR(50),
                [Mã đơn hàng] VARCHAR(16),
                [Số hóa đơn] VARCHAR(50),
                [Tên Merchant] VARCHAR(150),
                [Master MC] VARCHAR(16),
                [Mã - Tên Terminal] VARCHAR(70),
                [Đơn vị TT] VARCHAR(20),
                [Kênh thanh toán] NVARCHAR(16),
                [Phương thức thanh toán] VARCHAR(30),
                [Tên KH thanh toán] VARCHAR(100),
                [Số điện thoại] VARCHAR(16),
                [Số tài khoản] VARCHAR(30),
                [Loại thẻ/tài khoản] NVARCHAR(22),
                [Số tiền trước km] DECIMAL(18, 2),
                [Số tiền sau km] DECIMAL(18, 2),
                [Số tiền trước km (ngoại tệ)] DECIMAL(18, 2),
                [Số tiền sau km (ngoại tệ)] DECIMAL(18, 2),
                [Tổ chức tài trợ km] VARCHAR(16),
                [Tỷ giá] DECIMAL(18, 4),
                [Loại ngoại tệ] VARCHAR(5),
                [Thời gian thanh toán] DATETIME,
                [Trạng thái] VARCHAR(16),
                [Số trace phase 2] VARCHAR(35),
                [Phase 2 code] VARCHAR(12),
                [Số trace phase 3] VARCHAR(12),
                [Phase 3 code] VARCHAR(5),
                [Số trace phase 4] VARCHAR(30),
                [Phase 4 code] VARCHAR(5),
                [Mã phê duyệt] VARCHAR(8),
                [MCC nội địa] VARCHAR(8),
                [MCC quốc tế] VARCHAR(8),
                [Số tiền TIP] DECIMAL(18, 2),
                [Hình thức thẻ] VARCHAR(10),
                [SourceFile] VARCHAR(55),
                [ID] INT IDENTITY(1,1) PRIMARY KEY
            );
            
        END
        """
        
        # Execute each query separately
        with engine.connect() as conn:
            # Create schema first
            conn.execute(text(schema_check))
            conn.commit()
            print("Schema check completed")
            
            # Then create table if needed
            conn.execute(text(table_check))
            conn.commit()
            print("Table check completed")
        
        print("Database structure is ready")
        return True
        
    except Exception as e:
        print(f"Error preparing database: {str(e)}")
        raise

def load_and_process_file(file_path):
    """Load and process a single Excel file with optimized settings for large files"""
    try:
        print(f"  Loading file: {file_path}")
        
        # Try different Excel engines for large files
        try:
            # First try with engine='openpyxl' which is better for large files
            df = pd.read_excel(
                file_path, 
                dtype=str,
                engine='openpyxl'
            )
        except Exception as openpyxl_error:
            print(f"  Warning: openpyxl engine failed: {str(openpyxl_error)}")
            print("  Trying with default engine...")
            # Fall back to default engine
            df = pd.read_excel(file_path, dtype=str)
        
        if df.empty:
            print("  File is empty")
            return None
        
        rows = len(df)
        cols = len(df.columns)
        print(f"  File loaded: {rows:,} rows, {cols} columns")
        
        # For very large files, use optimized processing
        is_large_file = rows > 100000
        if is_large_file:
            print(f"  Large file detected ({rows:,} rows), using optimized processing...")
        
        # Clean numeric strings
        print("  Cleaning text values...")
        for col in df.columns:
            if df[col].notna().any():
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)
        
        # Convert date column
        print("  Converting date/time values...")
        if 'Thời gian thanh toán' in df.columns:
            df['Thời gian thanh toán'] = pd.to_datetime(
                df['Thời gian thanh toán'], 
                format='%d/%m/%Y %H:%M:%S', 
                errors='coerce'
            )
        
        # Convert numeric columns
        print("  Converting numeric values...")
        numeric_cols = ['STT', 'Số tiền trước km', 'Số tiền sau km', 
                       'Số tiền trước km (ngoại tệ)', 'Số tiền sau km (ngoại tệ)',
                       'Số tiền TIP', 'Tỷ giá']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Add source file
        df['SourceFile'] = os.path.basename(file_path)
        
        print("  File processing completed")
        return df
    
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None

def upload_to_sql(df, engine, table_name):
    """Upload DataFrame to SQL efficiently, handling very large files"""
    rows = len(df)
    print(f"  Uploading {rows:,} rows to database...")
    
    # Determine chunk size based on file size
    # SQL Server parameter limit issues typically happen around 2100 parameters
    # With 36 columns, that's about 58 rows per batch max
    if rows > 100000:
        chunksize = 3000  # Very small chunks for huge files
        print("  Very large file detected, using 30-row chunks for upload...")
    elif rows > 10000:
        chunksize = 5000  # Small chunks for large files
        print(f"  Large file detected, using 50-row chunks for upload...")
    else:
        chunksize = 10000  # Larger chunks for normal files
    
    # Track progress
    last_progress = 0
    start_time = time.time()
    rows_uploaded = 0
    
    try:
        # Upload in chunks
        for i in range(0, rows, chunksize):
            end_idx = min(i + chunksize, rows)
            chunk = df.iloc[i:end_idx]
            
            try:
                # Standard upload method
                chunk.to_sql(
                    name=table_name,
                    schema='vnpay',
                    con=engine,
                    if_exists='append',
                    index=False,
                    # Don't use method='multi' for large files
                    chunksize=None  # Disable internal chunking
                )
                rows_uploaded += len(chunk)
                
            except Exception as chunk_error:
                print(f"  Error uploading chunk at position {i}: {str(chunk_error)}")
                print("  Trying with individual row inserts...")
                
                # Try one row at a time as a last resort
                for idx, row in chunk.iterrows():
                    try:
                        row_df = pd.DataFrame([row])
                        row_df.to_sql(
                            name=table_name,
                            schema='vnpay',
                            con=engine,
                            if_exists='append',
                            index=False
                        )
                        rows_uploaded += 1
                    except Exception as row_error:
                        print(f"  Failed to insert row {idx}: {str(row_error)}")
            
            # Update progress for large files
            current_position = min(i + chunksize, rows)
            current_progress = (current_position / rows) * 100
            
            # Show progress every 2% or every 10000 rows for large files
            if (current_progress - last_progress >= 2) or (current_position % 10000 < chunksize):
                elapsed = time.time() - start_time
                rate = rows_uploaded / elapsed if elapsed > 0 else 0
                
                # Estimate remaining time
                if rate > 0:
                    remaining_rows = rows - rows_uploaded
                    remaining_seconds = remaining_rows / rate
                    remaining_mins = int(remaining_seconds // 60)
                    remaining_secs = int(remaining_seconds % 60)
                    
                    elapsed_mins = int(elapsed // 60)
                    elapsed_secs = int(elapsed % 60)
                    
                    print(f"  Progress: {current_progress:.1f}% ({rows_uploaded:,}/{rows:,} rows) | "
                          f"Rate: {rate:.1f} rows/sec | "
                          f"Elapsed: {elapsed_mins}m {elapsed_secs}s | "
                          f"Remaining: ~{remaining_mins}m {remaining_secs}s")
                    
                    last_progress = current_progress
    
        # Final timing
        total_time = time.time() - start_time
        total_mins = int(total_time // 60)
        total_secs = int(total_time % 60)
        
        if rows_uploaded == rows:
            print(f"  ✅ Upload completed: {rows_uploaded:,} rows in {total_mins}m {total_secs}s")
            return True
        else:
            print(f"  ⚠️ Partial upload: {rows_uploaded:,} of {rows:,} rows in {total_mins}m {total_secs}s")
            
            # Save remaining rows to CSV
            if rows_uploaded < rows:
                remaining = df.iloc[rows_uploaded:]
                remaining_filename = f"remaining_{os.path.basename(df['SourceFile'].iloc[0])}.csv"
                remaining.to_csv(remaining_filename, index=False, encoding='utf-8-sig')
                print(f"  Remaining {len(remaining):,} rows saved to {remaining_filename}")
            
            return rows_uploaded > 0  # Return True if at least some rows were uploaded
    
    except Exception as e:
        print(f"  ❌ Error during upload: {str(e)}")
        
        # Fall back to CSV
        csv_filename = f"failed_upload_{os.path.basename(df['SourceFile'].iloc[0])}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"  Data saved to {csv_filename}")
        return False

if __name__ == "__main__":
    process_and_upload_excel_files()
