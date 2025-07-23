from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, conlist
from typing import List
import boto3
import pandas as pd
import os
import tempfile
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
import uvicorn

# Import the new smart json generator
from smart_json_generator import generate_smart_json

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Smart JSON Generator",
    version="2.0.0",
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://localhost:3001",
        "https://api-irys.trophe.net",
        "https://irys.trophe.net",
        "https://brickroadapp.com",
        "https://staging-brickroadapp.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')
SMART_JSON_BUCKET = os.getenv('S3_BUCKET', 'irys-smart-json-output')  # Default bucket for smart JSON outputs
PARQUET_BUCKET = os.getenv('PARQUET_BUCKET', 'irys-parquet')

# S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def parse_s3_path(s3_path: str) -> tuple:
    """Parse an S3 path and return the bucket and key"""
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key

def download_from_s3(bucket: str, key: str, local_path: str) -> bool:
    """Download a file from S3"""
    try:
        s3_client.download_file(bucket, key, local_path)
        return True
    except Exception as e:
        print(f"Error during download: {e}")
        return False

def upload_json_to_s3(json_data: dict, bucket: str, key: str) -> bool:
    """Upload JSON data to S3"""
    try:
        json_string = json.dumps(json_data, indent=2)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_string,
            ContentType='application/json'
        )
        return True
    except Exception as e:
        print(f"Error during JSON upload: {e}")
        return False

def read_file_to_dataframe(input_path: str) -> pd.DataFrame:
    """Reads a file (Parquet, CSV, etc.) and returns it as a pandas DataFrame."""
    file_extension = os.path.splitext(input_path)[1].lower()
    
    if file_extension == '.parquet':
        return pd.read_parquet(input_path)
    elif file_extension == '.csv':
        return pd.read_csv(input_path)
    elif file_extension == '.json':
        return pd.read_json(input_path)
    elif file_extension in ['.xlsx', '.xls']:
        return pd.read_excel(input_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

class S3PathsInput(BaseModel):
    s3_paths: conlist(str, min_length=1)

@app.post('/generate-smart-json')
def generate_smart_json_endpoint(payload: S3PathsInput):
    """
    Endpoint to analyze multiple S3 files, combine them, and generate a SmartJSON.
    
    Expected JSON body:
    {
        "s3_paths": ["s3://bucket/path/to/file1.parquet", "s3://bucket/path/to/file2.parquet"]
    }
    """
    try:
        s3_paths = payload.s3_paths
            
        combined_dataframes = []
        total_raw_size = 0
        processed_files = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Process each file
            for i, s3_path in enumerate(s3_paths):
                try:
                    print(f"📥 [{i+1}/{len(s3_paths)}] Processing {s3_path}")
                    
                    source_bucket, source_key = parse_s3_path(s3_path)
                    
                    if not source_key:
                        print(f"⚠️  Invalid path ignored: {s3_path}")
                        continue
                    
                    original_filename = os.path.basename(source_key)
                    local_path = os.path.join(temp_dir, f"{i}_{original_filename}")
                    
                    # 1. Download the file
                    if not download_from_s3(source_bucket, source_key, local_path):
                        print(f"❌ Download failed: {s3_path}")
                        continue
                    
                    file_size = os.path.getsize(local_path)
                    total_raw_size += file_size
                    print(f"📊 Size: {file_size} bytes")

                    # 2. Read file into DataFrame
                    try:
                        df = read_file_to_dataframe(local_path)
                        combined_dataframes.append(df)
                        processed_files.append((source_bucket, source_key, s3_path))
                        print(f"✅ Added: {len(df)} rows, {len(df.columns)} columns")
                    except Exception as e:
                        print(f"❌ Error reading {s3_path}: {e}")
                        continue
                        
                except Exception as e:
                    print(f"❌ Error processing {s3_path}: {e}")
                    continue
            
            # Check that at least one file has been processed
            if not combined_dataframes:
                raise HTTPException(status_code=400, detail="No files could be processed successfully")
            
            # 3. Combine all DataFrames
            print(f"🔄 Combining {len(combined_dataframes)} files...")
            try:
                if len(combined_dataframes) == 1:
                    combined_df = combined_dataframes[0]
                else:
                    # Combine with concat, handling different columns
                    combined_df = pd.concat(combined_dataframes, ignore_index=True, sort=False)
                
                print(f"✅ Combined dataset: {len(combined_df)} rows, {len(combined_df.columns)} columns")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error during combination: {e}")

            # 4. Generate SmartJSON
            print(f"🔄 Generating SmartJSON...")
            smart_json_result = generate_smart_json(combined_df, total_raw_size)
            
            # 5. Upload SmartJSON to S3
            print(f"🔄 Uploading SmartJSON to S3...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            smart_json_key = f"smart-json/{timestamp}_{unique_id}.json"
            
            if not upload_json_to_s3(smart_json_result, SMART_JSON_BUCKET, smart_json_key):
                raise HTTPException(status_code=500, detail="Failed to upload SmartJSON to S3")
            
            smart_json_s3_path = f"s3://{SMART_JSON_BUCKET}/{smart_json_key}"
            print(f"✅ SmartJSON uploaded to: {smart_json_s3_path}")
            
            # 6. Return the S3 path instead of the full JSON
            response = {
                "smartJsonS3Path": smart_json_s3_path,
                "processedFiles": len(processed_files),
                "totalFiles": len(s3_paths),
                "combinedRows": len(combined_df),
                "combinedColumns": len(combined_df.columns)
            }
            print(f"✅ SmartJSON generated and uploaded successfully for {len(processed_files)} files")
            return response
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f'Internal error: {str(e)}')

@app.post('/convert-to-parquet')
def convert_to_parquet_endpoint(payload: S3PathsInput):
    """
    Endpoint to convert multiple S3 files to Parquet format and store them in S3.
    
    Expected JSON body:
    {
        "s3_paths": ["s3://bucket/path/to/file1.csv", "s3://bucket/path/to/file2.json"]
    }
    """
    try:
        s3_paths = payload.s3_paths
        
        converted_s3_paths = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            for i, s3_path in enumerate(s3_paths):
                try:
                    print(f"🔄 [{i+1}/{len(s3_paths)}] Converting {s3_path} to Parquet...")
                    
                    source_bucket, source_key = parse_s3_path(s3_path)
                    
                    if not source_key:
                        print(f"⚠️  Invalid path ignored: {s3_path}")
                        continue
                    
                    original_filename = os.path.basename(source_key)
                    local_input_path = os.path.join(temp_dir, f"input_{i}_{original_filename}")
                    
                    # 1. Download the file
                    if not download_from_s3(source_bucket, source_key, local_input_path):
                        print(f"❌ Download failed: {s3_path}")
                        continue
                    
                    # 2. Read file into DataFrame
                    try:
                        df = read_file_to_dataframe(local_input_path)
                    except Exception as e:
                        print(f"❌ Error reading {s3_path}: {e}")
                        continue
                        
                    # 3. Convert DataFrame to Parquet
                    parquet_filename = f"{os.path.splitext(original_filename)[0]}.parquet"
                    local_parquet_path = os.path.join(temp_dir, parquet_filename)
                    df.to_parquet(local_parquet_path, index=False)
                    print(f"✅ Converted {len(df)} rows to Parquet.")
                    
                    # 4. Upload Parquet to S3
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    unique_id = str(uuid.uuid4())[:8]
                    parquet_key = f"converted-parquet/{timestamp}_{unique_id}_{parquet_filename}"
                    
                    s3_client.upload_file(local_parquet_path, PARQUET_BUCKET, parquet_key)
                    
                    converted_s3_path = f"s3://{PARQUET_BUCKET}/{parquet_key}"
                    converted_s3_paths.append(converted_s3_path)
                    print(f"✅ Parquet uploaded to: {converted_s3_path}")
                    
                except Exception as e:
                    print(f"❌ Error processing {s3_path}: {e}")
                    continue
        
        if not converted_s3_paths:
            raise HTTPException(status_code=400, detail="No files could be converted successfully")
            
        return {
            "convertedParquetS3Paths": converted_s3_paths,
            "processedFiles": len(converted_s3_paths),
            "totalFiles": len(s3_paths)
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f'Internal error: {str(e)}')

@app.get('/health')
def health_check():
    """Health check endpoint"""
    return {'status': 'healthy', 'service': 'smart-json-generator'}

@app.get('/')
def home():
    """Root endpoint with documentation"""
    documentation = {
        'service': 'Smart JSON Generator',
        'version': '2.0.0',
        'endpoints': {
            '/generate-smart-json': {
                'method': 'POST',
                'description': 'Generates a SmartJSON from multiple combined S3 files and uploads it to S3',
                'body': {
                    's3_paths': 'List of S3 paths (required) - e.g.: ["s3://bucket/file1.parquet", "s3://bucket/file2.csv"]'
                },
                'response': {
                    'smartJsonS3Path': 'S3 path to the generated SmartJSON file',
                    'processedFiles': 'Number of files successfully processed',
                    'totalFiles': 'Total number of input files',
                    'combinedRows': 'Total rows in the combined dataset',
                    'combinedColumns': 'Total columns in the combined dataset'
                },
                'example': {
                    's3_paths': [
                        's3://my-bucket/data/users_part1.parquet',
                        's3://my-bucket/data/users_part2.parquet'
                    ]
                }
            },
            '/convert-to-parquet': {
                'method': 'POST',
                'description': 'Converts multiple S3 files to Parquet format and uploads them to S3',
                'body': {
                    's3_paths': 'List of S3 paths (required) - e.g.: ["s3://bucket/file1.csv", "s3://bucket/file2.json"]'
                },
                'response': {
                    'convertedParquetS3Paths': 'List of S3 paths to the generated Parquet files',
                    'processedFiles': 'Number of files successfully processed',
                    'totalFiles': 'Total number of input files'
                },
                'example': {
                    's3_paths': [
                        's3://my-bucket/data/sales.csv',
                        's3://my-bucket/data/products.json'
                    ]
                }
            },
            '/health': {
                'method': 'GET',
                'description': 'Checks the health of the service'
            }
        },
        'supported_formats': ['parquet', 'csv', 'json', 'xlsx', 'xls'],
        'features': [
            'Combine multiple files into single dataset',
            'Generate comprehensive dataset statistics',
            'Support for mixed file formats',
            'Upload SmartJSON results to S3',
            'Return S3 path for frontend management',
            'Preserve original files on S3'
        ],
        'cors': {
            'allowed_origins': ['http://localhost:3000', 'http://localhost:3001', ],
            'methods': ['GET', 'POST']
        }
    }
    return documentation

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5000, workers=4) 