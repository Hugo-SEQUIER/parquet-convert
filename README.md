# Parquet Convert API

API Python pour convertir des fichiers depuis S3 en format CSV.

## Fonctionnalités

- Récupère des fichiers depuis AWS S3
- Convertit les fichiers en format CSV avec pandas
- Supporte plusieurs formats d'entrée : Parquet, JSON, Excel (xlsx/xls), CSV
- Upload automatique des fichiers convertis vers S3
- API REST simple et intuitive

## Installation

1. Cloner le repository
```bash
git clone <repository-url>
cd parquet-convert
```

2. Installer les dépendances
```bash
pip install -r requirements.txt
```

3. Configurer les variables d'environnement
```bash
cp .env.example .env
# Éditer le fichier .env avec vos credentials AWS
```

## Configuration

Créer un fichier `.env` avec les variables suivantes :

```env
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=eu-west-1
S3_BUCKET=your-default-bucket
```

## Utilisation

### Démarrer l'API

```bash
python app.py
```

L'API sera disponible sur `http://localhost:5000`

### Endpoints

#### POST /convert

Convertit des fichiers S3 en CSV.

**Body JSON :**
```json
{
  "s3_paths": [
    "s3://my-bucket/data/file1.parquet",
    "s3://my-bucket/data/file2.json"
  ],
  "output_bucket": "my-output-bucket"  // optionnel
}
```

**Réponse :**
```json
{
  "results": [
    {
      "source_path": "s3://my-bucket/data/file1.parquet",
      "converted_path": "s3://my-output-bucket/converted/file1_uuid.csv",
      "status": "success"
    }
  ],
  "total_processed": 2,
  "successful": 1,
  "failed": 0
}
```

#### GET /health

Vérifie la santé du service.

#### GET /

Documentation de l'API.

### Exemple avec curl

```bash
curl -X POST http://localhost:5000/convert \
  -H "Content-Type: application/json" \
  -d '{
    "s3_paths": ["s3://my-bucket/data/file.parquet"],
    "output_bucket": "my-output-bucket"
  }'
```

## Formats supportés

- **Parquet** (.parquet)
- **JSON** (.json)
- **Excel** (.xlsx, .xls)
- **CSV** (.csv)

## Structure du projet

```
parquet-convert/
├── app.py              # Application Flask principale
├── requirements.txt    # Dépendances Python
├── .env.example       # Exemple de configuration
└── README.md          # Documentation
```

## Sécurité

- Utilisez des credentials AWS avec les permissions minimales nécessaires
- Ne commitez jamais le fichier `.env` avec vos vraies credentials
- Considérez l'utilisation d'IAM roles pour l'authentification en production

## Déploiement

Pour un déploiement en production, considérez :

- Utiliser un serveur WSGI (gunicorn)
- Configurer un load balancer
- Utiliser des variables d'environnement sécurisées
- Implémenter de la surveillance et des logs

```bash
# Exemple avec gunicorn
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
``` 