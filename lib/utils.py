import pandas as pd
import yaml
import os

yaml.Dumper.ignore_aliases = lambda *args : True  # Preserves key order by default

def load_metadata(filename: str) -> dict:
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Metadata file not found: {filename}")
        
    with open(filename, 'r', encoding='utf-8') as file:
        try:
            metadata = yaml.safe_load(file)
            print(f"Loaded metadata from {filename}")
            return metadata
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in metadata file: {str(e)}")
        

def save_metadata(filename: str, metadata: dict):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as file:
        yaml.dump(metadata, file, allow_unicode=True, sort_keys=False)
        print(f"Saved metadata to {filename}")


def get_table_info(context, schema: str, table: str) -> dict:
    query = f"""
    SELECT 
        regexp_replace(comment, ' \(Fonte:[^)]+\)', '') as description,
        NULLIF(regexp_extract(comment, 'Fonte: ([^)]+)', 1), '') as source
    FROM duckdb_views where view_name = '{table}' and schema_name = '{schema}'
    """
    return context.fetchdf(query).to_dict(orient='records')[0]


def get_columns_info(context, schema: str, table: str) -> pd.DataFrame:
    query = f"""
    SELECT 
        column_name,
        data_type as type,
        regexp_replace(comment, ' \(Fonte:[^)]+\)', '') as description,
        NULLIF(regexp_extract(comment, 'Fonte: ([^)]+)', 1), '') as source
    FROM duckdb_columns where table_name = '{table}' and schema_name = '{schema}'
    """
    return context.fetchdf(query)

def update_resource_metadata(context, key: str, resource: dict) -> dict:
    schema, table = key.split('.')
    resource['repr'] = f"## {key}\n"
    
    # Update table info
    table_info = get_table_info(context, schema, table)
    if table_info['description']:
        resource['description'] = table_info['description']
        resource['repr'] += f"> {table_info['description']}\n"
    if table_info['source'] is not None:
        resource['source'] = table_info['source']
    
    # Update column info
    fields = get_columns_info(context, schema, table)
    resource['schema.fields'] = [{k: v for k, v in field.items() if v is not None} 
                                for field in fields.to_dict(orient='records')]
    resource['repr'] += fields.to_markdown(index=False)
    
    return resource
def update_metadata(context, execution_time: datetime, model: str) -> pd.DataFrame:
    try:
        org, schema = model.split('.')
        filename = f'models/{org}/{schema}/meta/metadata.yaml'
        metadata = load_metadata(filename)
        
        for key, resource in metadata['resources'].items():
            metadata['resources'][key] = update_resource_metadata(context, key, resource)
        
        metadata['created'] = execution_time.strftime('%Y-%m-%d')
        
        save_metadata(filename, metadata)
        return pd.DataFrame([{"id": f"{org}__{schema}", "created_at": execution_time, "metadata": metadata}])
    
    except Exception as e:
        print(f"Error processing {model}: {str(e)}")
        raise
        
   