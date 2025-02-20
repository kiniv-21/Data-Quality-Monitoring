# dags/utils/metric_collectors.py
import pandas as pd
import sqlalchemy as sa
from datetime import datetime

def connect_to_db(connection_string):
    """Create a database connection"""
    engine = sa.create_engine(connection_string)
    return engine

def collect_completeness_metrics(engine, table_name, column_name):
    """Calculate completeness metrics for a specific column"""
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT({column_name}) as non_null_count,
        (COUNT({column_name})::float / COUNT(*)::float) * 100 as completeness_pct
    FROM {table_name}
    """
    df = pd.read_sql(query, engine)
    return {
        'metric_name': f'completeness_{table_name}_{column_name}',
        'table_name': table_name,
        'column_name': column_name,
        'total_rows': df['total_rows'][0],
        'non_null_count': df['non_null_count'][0],
        'completeness_pct': df['completeness_pct'][0],
        'timestamp': datetime.now().isoformat()
    }

def collect_uniqueness_metrics(engine, table_name, column_name):
    """Calculate uniqueness metrics for a specific column"""
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT {column_name}) as unique_count,
        (COUNT(DISTINCT {column_name})::float / COUNT(*)::float) * 100 as uniqueness_pct
    FROM {table_name}
    """
    df = pd.read_sql(query, engine)
    return {
        'metric_name': f'uniqueness_{table_name}_{column_name}',
        'table_name': table_name,
        'column_name': column_name,
        'total_rows': df['total_rows'][0],
        'unique_count': df['unique_count'][0],
        'uniqueness_pct': df['uniqueness_pct'][0],
        'timestamp': datetime.now().isoformat()
    }

def save_metrics_to_db(metrics, quality_db_engine):
    """Save collected metrics to the quality database"""
    df = pd.DataFrame([metrics])
    df.to_sql('data_quality_metrics', quality_db_engine, if_exists='append', index=False)
    print(f"Metrics saved: {metrics['metric_name']}")