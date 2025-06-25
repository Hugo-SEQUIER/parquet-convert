#!/usr/bin/env python3
"""
Script de debug pour diagnostiquer les problèmes de téléchargement S3
"""

import os
import tempfile
import boto3
from dotenv import load_dotenv
import pandas as pd

# Charger les variables d'environnement
load_dotenv()

def compare_files(local_file: str, s3_bucket: str, s3_key: str):
    """Compare un fichier local avec sa version S3"""
    
    print("🔍 DIAGNOSTIC S3 vs LOCAL")
    print("=" * 50)
    
    # Configuration S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )
    
    # 1. Analyser le fichier local
    print(f"\n📁 FICHIER LOCAL: {local_file}")
    if os.path.exists(local_file):
        local_size = os.path.getsize(local_file)
        print(f"   ✅ Taille: {local_size} bytes")
        
        # Lire les magic bytes
        with open(local_file, 'rb') as f:
            local_magic = f.read(16)
        print(f"   🔢 Magic bytes: {local_magic.hex()}")
        print(f"   📋 Est Parquet: {local_magic.startswith(b'PAR1')}")
        
        # Tester la lecture avec pandas
        try:
            df_local = pd.read_parquet(local_file)
            print(f"   ✅ Pandas peut le lire: {len(df_local)} lignes, {len(df_local.columns)} colonnes")
        except Exception as e:
            print(f"   ❌ Erreur pandas local: {e}")
    else:
        print(f"   ❌ Fichier local introuvable!")
        return
    
    # 2. Analyser le fichier S3
    print(f"\n☁️  FICHIER S3: s3://{s3_bucket}/{s3_key}")
    try:
        # Obtenir les métadonnées S3
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        s3_size = response['ContentLength']
        s3_etag = response.get('ETag', '').strip('"')
        print(f"   📊 Taille S3: {s3_size} bytes")
        print(f"   🏷️  ETag: {s3_etag}")
        
        # Télécharger et analyser
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            print(f"   📥 Téléchargement vers: {temp_path}")
            s3_client.download_file(s3_bucket, s3_key, temp_path)
            
            # Analyser le fichier téléchargé
            downloaded_size = os.path.getsize(temp_path)
            print(f"   📏 Taille téléchargée: {downloaded_size} bytes")
            
            with open(temp_path, 'rb') as f:
                downloaded_magic = f.read(16)
            print(f"   🔢 Magic bytes téléchargés: {downloaded_magic.hex()}")
            print(f"   📋 Est Parquet téléchargé: {downloaded_magic.startswith(b'PAR1')}")
            
            # Tester la lecture avec pandas
            try:
                df_downloaded = pd.read_parquet(temp_path)
                print(f"   ✅ Pandas peut lire le téléchargé: {len(df_downloaded)} lignes, {len(df_downloaded.columns)} colonnes")
            except Exception as e:
                print(f"   ❌ Erreur pandas téléchargé: {e}")
            
        finally:
            # Nettoyer
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except Exception as e:
        print(f"   ❌ Erreur S3: {e}")
    
    # 3. Comparaison
    print(f"\n🔗 COMPARAISON")
    print(f"   Tailles identiques: {local_size == s3_size if 's3_size' in locals() else 'Inconnu'}")
    print(f"   Magic bytes identiques: {local_magic == downloaded_magic if 'downloaded_magic' in locals() else 'Inconnu'}")

def test_specific_file():
    """Test avec le fichier spécifique mentionné"""
    local_file = "test/train-00002-of-00008.parquet"
    
    print("🧪 TEST FICHIER SPÉCIFIQUE")
    print("=" * 50)
    
    if os.path.exists(local_file):
        print(f"✅ Fichier local trouvé: {local_file}")
        
        # Demander les infos S3
        print("\n📝 Veuillez fournir les informations S3:")
        s3_bucket = input("Bucket S3: ").strip()
        s3_key = input("Clé S3 (chemin du fichier): ").strip()
        
        if s3_bucket and s3_key:
            compare_files(local_file, s3_bucket, s3_key)
        else:
            print("❌ Informations S3 manquantes")
    else:
        print(f"❌ Fichier local introuvable: {local_file}")

def test_file_upload():
    """Test upload du fichier local vers S3 et re-téléchargement"""
    local_file = "test/train-00000-of-00008.parquet"
    
    if not os.path.exists(local_file):
        print(f"❌ Fichier local introuvable: {local_file}")
        return
    
    print("📤 TEST UPLOAD/DOWNLOAD")
    print("=" * 50)
    
    # Configuration S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'eu-west-1')
    )
    
    bucket = os.getenv('S3_BUCKET')
    if not bucket:
        bucket = input("Bucket S3 pour le test: ").strip()
    
    test_key = f"debug/test-{os.path.basename(local_file)}"
    
    try:
        print(f"📤 Upload vers s3://{bucket}/{test_key}")
        s3_client.upload_file(local_file, bucket, test_key)
        print("✅ Upload réussi")
        
        # Immédiatement re-télécharger
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name
        
        try:
            print(f"📥 Re-téléchargement vers {temp_path}")
            s3_client.download_file(bucket, test_key, temp_path)
            
            # Comparer
            original_size = os.path.getsize(local_file)
            downloaded_size = os.path.getsize(temp_path)
            
            print(f"📊 Taille originale: {original_size} bytes")
            print(f"📊 Taille téléchargée: {downloaded_size} bytes")
            print(f"✅ Tailles identiques: {original_size == downloaded_size}")
            
            # Tester la lecture
            try:
                df = pd.read_parquet(temp_path)
                print(f"✅ Lecture pandas réussie: {len(df)} lignes")
            except Exception as e:
                print(f"❌ Erreur lecture pandas: {e}")
                
        finally:
            os.unlink(temp_path)
            
        # Nettoyer S3
        print(f"🧹 Suppression du fichier test de S3")
        s3_client.delete_object(Bucket=bucket, Key=test_key)
        
    except Exception as e:
        print(f"❌ Erreur durant le test: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "compare":
            test_specific_file()
        elif sys.argv[1] == "upload":
            test_file_upload()
        else:
            print("Usage: python debug_s3.py [compare|upload]")
    else:
        print("🔧 SCRIPT DE DEBUG S3")
        print("=" * 30)
        print("1. compare - Compare fichier local vs S3")
        print("2. upload  - Test upload/download complet")
        print()
        choice = input("Choisissez une option (1 ou 2): ").strip()
        
        if choice == "1":
            test_specific_file()
        elif choice == "2":
            test_file_upload()
        else:
            print("❌ Option invalide") 