from flask import Flask, request, jsonify
import boto3
import pandas as pd
import os
import tempfile
from typing import List, Dict
import uuid
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

app = Flask(__name__)

# Configuration AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')
S3_BUCKET = os.getenv('S3_BUCKET')

# Client S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def parse_s3_path(s3_path: str) -> tuple:
    """Parse un chemin S3 et retourne le bucket et la clé"""
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key

def download_from_s3(bucket: str, key: str, local_path: str) -> bool:
    """Télécharge un fichier depuis S3"""
    try:
        s3_client.download_file(bucket, key, local_path)
        return True
    except Exception as e:
        print(f"Erreur lors du téléchargement: {e}")
        return False

def upload_to_s3(local_path: str, bucket: str, key: str) -> bool:
    """Upload un fichier vers S3"""
    try:
        s3_client.upload_file(local_path, bucket, key)
        return True
    except Exception as e:
        print(f"Erreur lors de l'upload: {e}")
        return False

def convert_to_csv(input_path: str, output_path: str) -> bool:
    """Convertit un fichier parquet en CSV"""
    try:
        # Lire le fichier en fonction de son extension
        file_extension = os.path.splitext(input_path)[1].lower()
        
        if file_extension == '.parquet':
            df = pd.read_parquet(input_path)
        elif file_extension == '.json':
            df = pd.read_json(input_path)
        elif file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(input_path)
        elif file_extension == '.csv':
            # Si c'est déjà un CSV, on le recopie simplement
            df = pd.read_csv(input_path)
        else:
            print(f"Format de fichier non supporté: {file_extension}")
            return False
        
        # Sauvegarder en CSV
        df.to_csv(output_path, index=False)
        return True
    except Exception as e:
        print(f"Erreur lors de la conversion: {e}")
        return False

@app.route('/convert', methods=['POST'])
def convert_files():
    """
    Endpoint principal pour convertir des fichiers S3 en CSV
    
    Body JSON attendu:
    {
        "s3_paths": ["s3://bucket/path/file1.parquet", "s3://bucket/path/file2.parquet"],
        "output_bucket": "output-bucket" (optionnel, utilise S3_BUCKET par défaut)
    }
    """
    try:
        data = request.get_json()
        
        if not data or 's3_paths' not in data:
            return jsonify({'error': 'Le champ s3_paths est requis'}), 400
        
        s3_paths = data['s3_paths']
        output_bucket = data.get('output_bucket', S3_BUCKET)
        
        if not isinstance(s3_paths, list):
            return jsonify({'error': 's3_paths doit être une liste'}), 400
        
        results = []
        errors = []
        
        for s3_path in s3_paths:
            try:
                # Parser le chemin S3
                source_bucket, source_key = parse_s3_path(s3_path)
                
                # Générer un ID unique pour éviter les conflits
                file_id = str(uuid.uuid4())
                
                # Créer des chemins temporaires
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Nom du fichier original
                    original_filename = os.path.basename(source_key)
                    filename_without_ext = os.path.splitext(original_filename)[0]
                    
                    # Chemins locaux
                    input_local_path = os.path.join(temp_dir, f"{file_id}_input_{original_filename}")
                    output_local_path = os.path.join(temp_dir, f"{file_id}_output_{filename_without_ext}.csv")
                    
                    # Télécharger depuis S3
                    if not download_from_s3(source_bucket, source_key, input_local_path):
                        errors.append(f"Impossible de télécharger {s3_path}")
                        continue
                    
                    # Convertir en CSV
                    if not convert_to_csv(input_local_path, output_local_path):
                        errors.append(f"Impossible de convertir {s3_path}")
                        continue
                    
                    # Générer le chemin de destination S3
                    output_key = f"converted/{filename_without_ext}_{file_id}.csv"
                    
                    # Upload vers S3
                    if not upload_to_s3(output_local_path, output_bucket, output_key):
                        errors.append(f"Impossible d'uploader le fichier converti pour {s3_path}")
                        continue
                    
                    # Ajouter aux résultats
                    results.append({
                        'source_path': s3_path,
                        'converted_path': f"s3://{output_bucket}/{output_key}",
                        'status': 'success'
                    })
                    
            except Exception as e:
                errors.append(f"Erreur lors du traitement de {s3_path}: {str(e)}")
        
        response = {
            'results': results,
            'total_processed': len(s3_paths),
            'successful': len(results),
            'failed': len(errors)
        }
        
        if errors:
            response['errors'] = errors
        
        return jsonify(response), 200 if not errors else 207  # 207 = Multi-Status
        
    except Exception as e:
        return jsonify({'error': f'Erreur interne: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de santé"""
    return jsonify({'status': 'healthy', 'service': 'parquet-convert-api'}), 200

@app.route('/', methods=['GET'])
def home():
    """Endpoint racine avec documentation"""
    documentation = {
        'service': 'Parquet Convert API',
        'version': '1.0.0',
        'endpoints': {
            '/convert': {
                'method': 'POST',
                'description': 'Convertit des fichiers S3 en CSV',
                'body': {
                    's3_paths': 'Liste des chemins S3 (requis)',
                    'output_bucket': 'Bucket de destination (optionnel)'
                }
            },
            '/health': {
                'method': 'GET',
                'description': 'Vérifie la santé du service'
            }
        },
        'supported_formats': ['parquet', 'json', 'xlsx', 'xls', 'csv']
    }
    return jsonify(documentation), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 