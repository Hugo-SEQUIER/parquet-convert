from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
import pandas as pd
import os
import tempfile
from dotenv import load_dotenv

# Import the new smart json generator
from smart_json_generator import generate_smart_json

# Charger les variables d'environnement
load_dotenv()

app = Flask(__name__)

# Configuration CORS
CORS(app, 
     origins=["http://localhost:3000", "http://localhost:3001"],
     supports_credentials=True,
     methods=["GET", "POST"],
     allow_headers=["*"])

# Configuration AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')

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

def read_file_to_dataframe(input_path: str) -> pd.DataFrame:
    """Lit un fichier (Parquet, CSV, etc.) et le retourne en DataFrame pandas."""
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
        raise ValueError(f"Format de fichier non supporté: {file_extension}")

@app.route('/generate-smart-json', methods=['POST'])
def generate_smart_json_endpoint():
    """
    Endpoint pour analyser plusieurs fichiers S3, les combiner et générer un SmartJSON.
    
    Body JSON attendu:
    {
        "s3_paths": ["s3://bucket/path/to/file1.parquet", "s3://bucket/path/to/file2.parquet"]
    }
    """
    try:
        data = request.get_json()
        
        if not data or 's3_paths' not in data:
            return jsonify({'error': 'Le champ s3_paths est requis'}), 400
        
        s3_paths = data['s3_paths']
        
        if not isinstance(s3_paths, list) or len(s3_paths) == 0:
            return jsonify({'error': 's3_paths doit être une liste non-vide'}), 400
            
        combined_dataframes = []
        total_raw_size = 0
        processed_files = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Traiter chaque fichier
            for i, s3_path in enumerate(s3_paths):
                try:
                    print(f"📥 [{i+1}/{len(s3_paths)}] Traitement de {s3_path}")
                    
                    source_bucket, source_key = parse_s3_path(s3_path)
                    
                    if not source_key:
                        print(f"⚠️  Chemin invalide ignoré: {s3_path}")
                        continue
                    
                    original_filename = os.path.basename(source_key)
                    local_path = os.path.join(temp_dir, f"{i}_{original_filename}")
                    
                    # 1. Télécharger le fichier
                    if not download_from_s3(source_bucket, source_key, local_path):
                        print(f"❌ Échec téléchargement: {s3_path}")
                        continue
                    
                    file_size = os.path.getsize(local_path)
                    total_raw_size += file_size
                    print(f"📊 Taille: {file_size} bytes")

                    # 2. Lire le fichier en DataFrame
                    try:
                        df = read_file_to_dataframe(local_path)
                        combined_dataframes.append(df)
                        processed_files.append((source_bucket, source_key, s3_path))
                        print(f"✅ Ajouté: {len(df)} lignes, {len(df.columns)} colonnes")
                    except Exception as e:
                        print(f"❌ Erreur lecture {s3_path}: {e}")
                        continue
                        
                except Exception as e:
                    print(f"❌ Erreur traitement {s3_path}: {e}")
                    continue
            
            # Vérifier qu'on a au moins un fichier traité
            if not combined_dataframes:
                return jsonify({'error': 'Aucun fichier n\'a pu être traité avec succès'}), 400
            
            # 3. Combiner tous les DataFrames
            print(f"🔄 Combinaison de {len(combined_dataframes)} fichiers...")
            try:
                if len(combined_dataframes) == 1:
                    combined_df = combined_dataframes[0]
                else:
                    # Combiner avec concat en gérant les colonnes différentes
                    combined_df = pd.concat(combined_dataframes, ignore_index=True, sort=False)
                
                print(f"✅ Dataset combiné: {len(combined_df)} lignes, {len(combined_df.columns)} colonnes")
            except Exception as e:
                return jsonify({'error': f"Erreur lors de la combinaison: {e}"}), 500

            # 4. Générer le SmartJSON
            print(f"🔄 Génération du SmartJSON...")
            smart_json_result = generate_smart_json(combined_df, total_raw_size)
            
            # 5. Retourner le résultat (sans supprimer les fichiers S3)
            response = {
                "smartJson": smart_json_result,
                "processedFiles": len(processed_files),
                "totalFiles": len(s3_paths),
                "combinedRows": len(combined_df),
                "combinedColumns": len(combined_df.columns)
            }
            print(f"✅ SmartJSON généré avec succès pour {len(processed_files)} fichiers")
            return jsonify(response), 200
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Erreur interne: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de santé"""
    return jsonify({'status': 'healthy', 'service': 'smart-json-generator'}), 200

@app.route('/', methods=['GET'])
def home():
    """Endpoint racine avec documentation"""
    documentation = {
        'service': 'Smart JSON Generator',
        'version': '2.0.0',
        'endpoints': {
            '/generate-smart-json': {
                'method': 'POST',
                'description': 'Génère un SmartJSON à partir de plusieurs fichiers S3 combinés',
                'body': {
                    's3_paths': 'Liste des chemins S3 (requis) - ex: ["s3://bucket/file1.parquet", "s3://bucket/file2.csv"]'
                },
                'example': {
                    's3_paths': [
                        's3://mon-bucket/data/users_part1.parquet',
                        's3://mon-bucket/data/users_part2.parquet'
                    ]
                }
            },
            '/health': {
                'method': 'GET',
                'description': 'Vérifie la santé du service'
            }
        },
        'supported_formats': ['parquet', 'csv', 'json', 'xlsx', 'xls'],
        'features': [
            'Combine multiple files into single dataset',
            'Generate comprehensive dataset statistics',
            'Support for mixed file formats',
            'Preserve original files on S3'
        ],
        'cors': {
            'allowed_origins': ['http://localhost:3000', 'http://localhost:3001'],
            'methods': ['GET', 'POST']
        }
    }
    return jsonify(documentation), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 