import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Union

# --- MongoDB-like Format Helpers ---

def format_number(value: Any) -> Dict[str, Any]:
    """Formats a number in MongoDB-like format."""
    if pd.isna(value):
        return None
    
    if isinstance(value, (int, np.integer)):
        return {"$numberInt": str(int(value))}
    elif isinstance(value, (float, np.floating)):
        if float(value).is_integer():
            return {"$numberInt": str(int(value))}
        return {"$numberDouble": str(float(value))}
    else:
        # Try to convert
        try:
            num_val = float(value)
            if num_val.is_integer():
                return {"$numberInt": str(int(num_val))}
            return {"$numberDouble": str(num_val)}
        except:
            return str(value)

# --- Type Detection ---

def detect_column_type(series: pd.Series) -> str:
    """Detects the data type of a pandas Series."""
    if series.dtype == 'object':
        series = series.dropna().astype(str)
        if series.empty:
            return "string"
        
        sample = series.head(100)

        # 1. Datetime
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'^\d{2}\/\d{2}\/\d{4}',  # MM/DD/YYYY
            r'^\d{4}\/\d{2}\/\d{2}',  # YYYY/MM/DD
            r'^\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'^\d{4}$'  # Just year
        ]
        date_count = sample.str.match('|'.join(date_patterns)).sum()
        if date_count / len(sample) > 0.8:
            return "datetime"

        # 2. Boolean
        bool_vals = {"true", "false", "1", "0", "yes", "no"}
        boolean_count = sample.str.lower().isin(bool_vals).sum()
        if boolean_count / len(sample) > 0.8:
            return "boolean"
        
        # 3. Numeric (try converting to numbers)
        numeric_series = pd.to_numeric(sample, errors='coerce')
        nan_count = numeric_series.isna().sum()
        numeric_ratio = (len(sample) - nan_count) / len(sample)
        
        if numeric_ratio > 0.8:
            # Check if mostly integers
            int_count = numeric_series.dropna().apply(lambda x: float(x).is_integer()).sum()
            if int_count / (len(sample) - nan_count) > 0.8:
                return "int"
            return "float"

    elif pd.api.types.is_integer_dtype(series):
        return "int"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    elif pd.api.types.is_bool_dtype(series):
        return "boolean"
        
    return "string"


# --- Numeric Stats ---

def generate_numeric_stats(series: pd.Series) -> Dict[str, Any]:
    """Generates statistics for a numeric column."""
    series = pd.to_numeric(series.dropna(), errors='coerce').dropna()
    
    if series.empty:
        return {
            'count': format_number(0),
            'mean': format_number(0.0),
            'std': format_number(0.0),
            'min': format_number(0),
            'q25': format_number(0),
            'median': format_number(0),
            'q75': format_number(0),
            'max': format_number(0),
            'histBins': [],
            'histCounts': []
        }

    stats = series.describe()
    hist_counts, hist_bins = np.histogram(series, bins=10)

    return {
        'count': format_number(int(stats.get('count', 0))),
        'mean': format_number(stats.get('mean', 0)),
        'std': format_number(stats.get('std', 0)),
        'min': format_number(stats.get('min', 0)),
        'q25': format_number(stats.get('25%', 0)),
        'median': format_number(stats.get('50%', 0)),
        'q75': format_number(stats.get('75%', 0)),
        'max': format_number(stats.get('max', 0)),
        'histBins': [format_number(x) for x in hist_bins.tolist()],
        'histCounts': [format_number(int(x)) for x in hist_counts.tolist()]
    }

# --- Category Stats ---

def generate_category_stats(series: pd.Series, column_name: str) -> Dict[str, Any]:
    """Generates statistics for a categorical column."""
    series = series.dropna().astype(str)
    
    # Check if this might be a large data column (images, binary data, etc.)
    if len(series) > 0:
        avg_length = series.str.len().mean()
        max_length = series.str.len().max()
        
        # If average length > 1000 chars or max > 10000, likely binary/image data
        if avg_length > 1000 or max_length > 10000:
            print(f"    ⚠️  Large data detected in {column_name} (avg: {avg_length:.0f}, max: {max_length:.0f} chars)")
            return {
                'top': [{'value': f'<large_data_avg_{avg_length:.0f}_chars>', 'count': format_number(len(series))}],
                'other': format_number(0)
            }
    
    try:
        counts = series.value_counts()
        
        if len(counts) <= 3:
            top = [{'value': k, 'count': format_number(v)} for k, v in counts.items()]
            return {'top': top, 'other': format_number(0)}

        # Smart grouping
        smart_cats = generate_smart_categories(series, column_name)
        if smart_cats:
            return smart_cats

        # Fallback: group by length
        length_counts = series.str.len().value_counts()
        top_lengths = length_counts.head(5)
        top = [{'value': f'{k} chars', 'count': format_number(v)} for k, v in top_lengths.items()]
        other = max(0, len(length_counts) - 5)
        
        return {'top': top, 'other': format_number(other)}
        
    except Exception as e:
        print(f"    ❌ Category stats failed for {column_name}: {e}")
        # Return safe fallback
        return {
            'top': [{'value': '<analysis_failed>', 'count': format_number(len(series))}],
            'other': format_number(0)
        }

def generate_smart_categories(series: pd.Series, column_name: str) -> Union[Dict, None]:
    """Attempt to generate 'smart' categories for dates, IDs, or emails."""
    # Skip smart categorization for large data
    if len(series) > 0:
        avg_length = series.str.len().mean()
        if avg_length > 500:  # Skip smart categories for large data
            return None
    
    try:
        sample = series.head(100)

        # Date-like
        if is_date_like_column(sample):
            result = generate_date_categories(series)
            if result:
                return result
        # ID-like
        if is_id_like_column(sample, column_name):
            result = generate_id_categories(series)
            if result:
                return result
        # Email-like
        if is_email_like_column(sample):
            result = generate_email_categories(series)
            if result:
                return result
            
        return None
        
    except Exception as e:
        print(f"    ⚠️  Smart categories failed for {column_name}: {e}")
        return None

def is_date_like_column(sample: pd.Series) -> bool:
    date_patterns = [r'^\d{4}-\d{2}-\d{2}', r'^\d{2}\/\d{2}\/\d{4}', r'^\d{4}$']
    match_count = sample.str.match('|'.join(date_patterns)).sum()
    return match_count / len(sample) > 0.7 if len(sample) > 0 else False

def generate_date_categories(series: pd.Series) -> Union[Dict, None]:
    dates = pd.to_datetime(series, errors='coerce').dropna()
    
    year_counts = dates.dt.year.value_counts()
    if len(year_counts) > 1:
        top = [{'value': str(k), 'count': format_number(v)} for k, v in year_counts.head(5).items()]
        return {'top': top, 'other': format_number(max(0, len(year_counts) - 5))}
        
    month_counts = dates.dt.strftime('%B').value_counts()
    if len(month_counts) > 1:
        top = [{'value': k, 'count': format_number(v)} for k, v in month_counts.head(5).items()]
        return {'top': top, 'other': format_number(max(0, len(month_counts) - 5))}

    return None

def is_id_like_column(sample: pd.Series, column_name: str) -> bool:
    if any(kw in column_name.lower() for kw in ["id", "uuid", "key", "token"]):
        return True
    id_like = sample.str.contains(r'[A-Za-z]', na=False) & sample.str.contains(r'\d', na=False) & (sample.str.len() > 8)
    return id_like.sum() / len(sample) > 0.8 if len(sample) > 0 else False

def generate_id_categories(series: pd.Series) -> Union[Dict, None]:
    length_counts = series.str.len().value_counts()
    if 1 < length_counts.size <= 4:
        top = [{'value': f'{k} chars', 'count': format_number(v)} for k, v in length_counts.items()]
        return {'top': top, 'other': format_number(0)}
        
    prefix_counts = series.str[:3].value_counts()
    if 1 < prefix_counts.size <= 8:
        top = [{'value': f'{k}*', 'count': format_number(v)} for k, v in prefix_counts.head(4).items()]
        return {'top': top, 'other': format_number(max(0, len(prefix_counts) - 4))}

    return None

def is_email_like_column(sample: pd.Series) -> bool:
    email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    return sample.str.match(email_pattern).sum() / len(sample) > 0.8 if len(sample) > 0 else False

def generate_email_categories(series: pd.Series) -> Union[Dict, None]:
    domains = series.str.split('@').str[1].value_counts()
    if domains.empty:
        return None
    top = [{'value': f'@{k}', 'count': format_number(v)} for k, v in domains.head(4).items()]
    return {'top': top, 'other': format_number(max(0, len(domains) - 4))}

# --- Correlations ---

def generate_correlations(df: pd.DataFrame) -> Union[Dict, None]:
    """Generates Pearson correlation block for numeric columns."""
    numeric_cols = df.select_dtypes(include=np.number)
    if numeric_cols.shape[1] < 2:
        return None
        
    corr_matrix = numeric_cols.corr(method='pearson')
    
    # Extract upper triangle
    features = corr_matrix.columns.tolist()
    values = []
    for i in range(len(features)):
        for j in range(i + 1, len(features)):
            corr_val = corr_matrix.iloc[i, j]
            if pd.isna(corr_val):
                values.append(format_number(0))
            else:
                values.append(format_number(round(corr_val, 3)))
            
    return {
        'method': 'pearson',
        'features': features,
        'values': values
    }

# --- Main Generator ---

def analyze_column(series: pd.Series, column_name: str) -> Dict[str, Any]:
    """Analyzes a single column and returns its profile."""
    try:
        print(f"    🔍 Analyzing missing values for {column_name}...")
        missing = series.isnull().sum()
        non_null_series = series.dropna()
        
        # Handle complex data types (dicts, lists) that are unhashable
        print(f"    🔍 Counting unique values for {column_name}...")
        try:
            unique = non_null_series.nunique()
        except TypeError:
            print(f"    ⚠️  Converting to string for unique count: {column_name}")
            # Convert complex objects to strings for unique counting
            unique = non_null_series.astype(str).nunique()
        
        print(f"    🔍 Detecting type for {column_name}...")
        col_type = detect_column_type(series)
        
        # For sample values, convert complex objects to strings
        print(f"    🔍 Extracting samples for {column_name}...")
        try:
            sample_values = non_null_series.head(3).astype(str).tolist()
        except:
            sample_values = ["<complex_object>", "<complex_object>", "<complex_object>"][:len(non_null_series.head(3))]

        profile = {
            'name': column_name,
            'type': col_type,
            'missing': format_number(missing),
            'unique': format_number(unique),
            'sample': sample_values
        }
        
        if col_type in ['int', 'float']:
            print(f"    🔍 Generating numeric stats for {column_name}...")
            try:
                profile['numericStats'] = generate_numeric_stats(series)
            except Exception as e:
                print(f"    ❌ Failed numeric stats for {column_name}: {e}")
        elif col_type in ['string', 'boolean', 'datetime']:
            print(f"    🔍 Generating category stats for {column_name}...")
            try:
                categories = generate_category_stats(series, column_name)
                if categories:
                    profile['categories'] = categories
            except Exception as e:
                print(f"    ❌ Failed category stats for {column_name}: {e}")
        
        return profile
        
    except Exception as e:
        print(f"    ❌ Complete failure analyzing {column_name}: {e}")
        # Return minimal profile
        return {
            'name': column_name,
            'type': 'unknown',
            'missing': format_number(0),
            'unique': format_number(0),
            'sample': ["<analysis_failed>"]
        }

def generate_smart_json(df: pd.DataFrame, raw_size_bytes: int, sample_size: int = 50) -> Dict[str, Any]:
    """Main function to generate SmartJSON from a pandas DataFrame."""
    print(f"🔍 Starting SmartJSON generation...")
    n_rows, n_features = df.shape
    print(f"📊 Dataset: {n_rows} rows, {n_features} columns")
    
    # Analyze columns one by one with progress
    columns = []
    print(f"🔄 Analyzing {n_features} columns...")
    
    for i, col in enumerate(df.columns):
        try:
            print(f"  📋 [{i+1}/{n_features}] Analyzing column: {col}")
            column_profile = analyze_column(df[col], col)
            columns.append(column_profile)
            print(f"  ✅ [{i+1}/{n_features}] Done: {col} ({column_profile['type']})")
        except Exception as e:
            print(f"  ❌ [{i+1}/{n_features}] Failed: {col} - {e}")
            # Create a minimal profile for failed columns
            columns.append({
                'name': col,
                'type': 'unknown',
                'missing': format_number(0),
                'unique': format_number(0),
                'sample': ["<analysis_failed>"]
            })
    
    # Calculate total missing values - convert from MongoDB format to int
    print(f"🔄 Calculating missing values...")
    total_missing = 0
    for c in columns:
        try:
            missing_val = c['missing']
            if isinstance(missing_val, dict) and '$numberInt' in missing_val:
                total_missing += int(missing_val['$numberInt'])
            else:
                total_missing += int(missing_val)
        except:
            pass  # Skip if conversion fails
    print(f"📊 Total missing: {total_missing}")

    # Count duplicates - handle complex data types safely
    print(f"🔄 Counting duplicates...")
    try:
        # Try to count duplicates on string representation if complex objects exist
        df_str = df.astype(str)
        duplicates = df_str.duplicated().sum()
        print(f"📊 Duplicates found: {duplicates}")
    except Exception as e:
        print(f"⚠️  Could not count duplicates: {e}")
        duplicates = 0
    
    # Correlations (only for datasets with numeric columns)
    print(f"🔄 Generating correlations...")
    try:
        correlations = generate_correlations(df)
        if correlations:
            print(f"📊 Correlations generated for {len(correlations['features'])} numeric columns")
        else:
            print(f"📊 No correlations (insufficient numeric columns)")
    except Exception as e:
        print(f"⚠️  Could not generate correlations: {e}")
        correlations = None
    
    # Sample rows - convert to strings for JSON serialization
    print(f"🔄 Preparing sample rows...")
    try:
        sample_rows = df.head(min(sample_size, n_rows)).astype(str).to_dict(orient='records')
        print(f"📊 Sample rows prepared: {len(sample_rows)} rows")
    except Exception as e:
        print(f"⚠️  Could not prepare sample rows: {e}")
        # If conversion fails, create simple sample
        sample_rows = [{"info": "Complex data structure - view original file"} for _ in range(min(3, n_rows))]

    # Build stats object like HuggingFace format
    print(f"🔄 Building final stats object...")
    try:
        stats = {
            'nRows': format_number(n_rows),
            'nFeatures': format_number(n_features),
            'missing': format_number(total_missing),
            'duplicates': format_number(duplicates),
            'sizeGb': format_number(raw_size_bytes / (1024**3)),
            'columns': columns
        }
        
        # Only add correlations if they exist (numeric columns present)
        if correlations:
            stats['correlations'] = correlations
        
        result = {'stats': stats, 'sampleRows': sample_rows}
        print(f"✅ SmartJSON generation completed successfully!")
        return result
        
    except Exception as e:
        print(f"❌ Failed to build final stats: {e}")
        # Return minimal result
        return {
            'stats': {
                'nRows': format_number(n_rows),
                'nFeatures': format_number(n_features),
                'missing': format_number(0),
                'duplicates': format_number(0),
                'sizeGb': format_number(raw_size_bytes / (1024**3)),
                'columns': []
            },
            'sampleRows': []
        } 