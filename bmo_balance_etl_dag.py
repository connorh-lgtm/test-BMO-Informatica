"""
BMO Financial Balance ETL Pipeline - Airflow DAG
Converted from Informatica PowerCenter implementation

This DAG replicates the complete ETL pipeline for BMO Financial Balance processing,
preserving all business logic including critical date validation abort logic.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
import numpy as np
from decimal import Decimal
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'bmo-etl-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'bmo_balance_etl',
    default_args=default_args,
    description='BMO Financial Balance ETL Pipeline - Converted from Informatica PowerCenter',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['bmo', 'financial', 'balance', 'etl'],
)

SOURCE_FILE_PATH = '/data/input/mrp0455_inv_gl.txt'
TARGET_TABLE = 'PS_HBC_INV_BAL_STG'
LOOKUP_TABLE = 'PS_HBC_PERIOD_CTL'
ORACLE_CONN_ID = 'oracle_bmo_db'

def validate_source_file(**context) -> str:
    """
    Validates the existence and accessibility of the source file.
    Replicates the source file validation from Informatica PowerCenter.
    """
    if not os.path.exists(SOURCE_FILE_PATH):
        raise AirflowException(f"Source file not found: {SOURCE_FILE_PATH}")
    
    if not os.access(SOURCE_FILE_PATH, os.R_OK):
        raise AirflowException(f"Source file not readable: {SOURCE_FILE_PATH}")
    
    file_size = os.path.getsize(SOURCE_FILE_PATH)
    if file_size == 0:
        raise AirflowException(f"Source file is empty: {SOURCE_FILE_PATH}")
    
    context['task_instance'].log.info(f"Source file validated successfully: {SOURCE_FILE_PATH} ({file_size} bytes)")
    return SOURCE_FILE_PATH

def extract_source_data(**context) -> Dict[str, Any]:
    """
    Extracts data from the flat file source.
    Replicates the Source Qualifier transformation from Informatica PowerCenter.
    
    Source fields based on Informatica_PowerCenter_Mapping.xml:
    - Effective_Date (string, 8 chars, offset 0)
    - Receipt_Number (string, 20 chars, offset 8) 
    - Balance_Type (string, 10 chars, offset 28)
    - Amount (decimal 15,3, offset 38)
    - Account (string, 20 chars, offset 53)
    """
    try:
        colspecs = [
            (0, 8),    # Effective_Date
            (8, 28),   # Receipt_Number
            (28, 38),  # Balance_Type
            (38, 53),  # Amount
            (53, 73),  # Account
        ]
        
        column_names = ['Effective_Date', 'Receipt_Number', 'Balance_Type', 'Amount', 'Account']
        
        df = pd.read_fwf(
            SOURCE_FILE_PATH,
            colspecs=colspecs,
            names=column_names,
            dtype={
                'Effective_Date': str,
                'Receipt_Number': str,
                'Balance_Type': str,
                'Amount': str,
                'Account': str
            },
            skiprows=1  # Skip header row if present
        )
        
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        
        string_cols = ['Effective_Date', 'Receipt_Number', 'Balance_Type', 'Account']
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
        
        context['task_instance'].log.info(f"Extracted {len(df)} records from source file")
        
        return {
            'data': df.to_dict('records'),
            'record_count': len(df)
        }
        
    except Exception as e:
        raise AirflowException(f"Failed to extract source data: {str(e)}")

def filter_null_receipts(**context) -> Dict[str, Any]:
    """
    Filters out records with NULL receipt numbers.
    Replicates the FIL_Remove_Null_Receipts transformation from Informatica PowerCenter.
    Filter condition: NOT ISNULL(Receipt_Number)
    """
    ti = context['task_instance']
    source_data = ti.xcom_pull(task_ids='extract_source_data')
    
    df = pd.DataFrame(source_data['data'])
    original_count = len(df)
    
    df_filtered = df[df['Receipt_Number'].notna() & (df['Receipt_Number'] != '') & (df['Receipt_Number'] != 'None')]
    
    filtered_count = len(df_filtered)
    removed_count = original_count - filtered_count
    
    context['task_instance'].log.info(f"Filtered records: {original_count} -> {filtered_count} (removed {removed_count} NULL receipts)")
    
    return {
        'data': df_filtered.to_dict('records'),
        'record_count': filtered_count,
        'removed_count': removed_count
    }

def lookup_period_control(**context) -> Dict[str, Any]:
    """
    Performs lookup against PS_HBC_PERIOD_CTL table.
    Replicates the LKP_HBC_PERIOD_CTL transformation from Informatica PowerCenter.
    SQL: SELECT HBC_DATA_DATE FROM PS_HBC_PERIOD_CTL WHERE RTRIM(HBC_PER_CTL) = 'APS'
    """
    try:
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        
        sql = "SELECT HBC_DATA_DATE FROM PS_HBC_PERIOD_CTL WHERE RTRIM(HBC_PER_CTL) = 'APS'"
        result = oracle_hook.get_first(sql)
        
        if not result or result[0] is None:
            raise AirflowException("Lookup failed: No HBC_DATA_DATE found for APS period control")
        
        hbc_data_date = result[0]
        context['task_instance'].log.info(f"Lookup successful: HBC_DATA_DATE = {hbc_data_date}")
        
        return {
            'hbc_data_date': hbc_data_date.strftime('%Y-%m-%d') if hasattr(hbc_data_date, 'strftime') else str(hbc_data_date)
        }
        
    except Exception as e:
        raise AirflowException(f"Period control lookup failed: {str(e)}")

def validate_dates_and_transform(**context) -> Dict[str, Any]:
    """
    Validates dates and performs field transformations.
    Replicates the EXP_Check_Date_Build_Table transformation from Informatica PowerCenter.
    
    Critical business logic:
    1. Date validation with abort logic
    2. Field construction and transformations
    3. Sequence number generation
    """
    ti = context['task_instance']
    filtered_data = ti.xcom_pull(task_ids='filter_null_receipts')
    lookup_data = ti.xcom_pull(task_ids='lookup_period_control')
    
    df = pd.DataFrame(filtered_data['data'])
    hbc_data_date = pd.to_datetime(lookup_data['hbc_data_date']).date()
    
    df['var_Header_Date'] = pd.to_datetime(df['Effective_Date'], format='%Y%m%d', errors='coerce').dt.date
    
    date_mismatches = df[df['var_Header_Date'] != hbc_data_date]
    if not date_mismatches.empty:
        mismatch_dates = date_mismatches['var_Header_Date'].unique()
        raise AirflowException(
            f"Intrader Effective Date does not equal the HBC_DATA_DATE on the APS Period Control row. "
            f"Expected: {hbc_data_date}, Found: {mismatch_dates}"
        )
    
    context['task_instance'].log.info(f"Date validation passed: All {len(df)} records match HBC_DATA_DATE {hbc_data_date}")
    
    df['SEQUENCENO'] = range(1, len(df) + 1)
    
    
    df['FI_INSTRUMENT_ID'] = 'INV' + df['Receipt_Number'].astype(str).str.zfill(42)
    
    df['FI_IBALTYPE_CD'] = df['Balance_Type'].apply(
        lambda x: ' ' if pd.isna(x) or x == '' or x == 'None' else '\n' + str(x).upper()
    )
    
    df['ACCOUNT'] = df['Account'].apply(
        lambda x: ' ' if pd.isna(x) or x == '' or x == 'None' else str(x)
    )
    
    df['PF_TRANS_DT'] = hbc_data_date
    
    df['HBC_RECEIPT_NO'] = df['Receipt_Number'].apply(
        lambda x: ' ' if pd.isna(x) or x == '' or x == 'None' else '\n' + str(x)
    )
    
    df['FI_BALANCE_AMT'] = df['Amount'].apply(
        lambda x: Decimal('0.000') if pd.isna(x) else Decimal(str(x)).quantize(Decimal('0.001'))
    )
    
    target_columns = [
        'FI_INSTRUMENT_ID', 'FI_IBALTYPE_CD', 'ACCOUNT', 'SEQUENCENO', 
        'PF_TRANS_DT', 'HBC_RECEIPT_NO', 'FI_BALANCE_AMT'
    ]
    
    df_target = df[target_columns].copy()
    
    context['task_instance'].log.info(f"Transformation completed: {len(df_target)} records ready for loading")
    
    return {
        'data': df_target.to_dict('records'),
        'record_count': len(df_target)
    }

def load_target_data(**context) -> Dict[str, Any]:
    """
    Loads transformed data into the target table.
    Replicates the target loading functionality from Informatica PowerCenter.
    Target: PS_HBC_INV_BAL_STG (Oracle table with TRUNCATE option)
    """
    ti = context['task_instance']
    transform_data = ti.xcom_pull(task_ids='validate_dates_and_transform')
    
    df = pd.DataFrame(transform_data['data'])
    
    if df.empty:
        context['task_instance'].log.warning("No data to load - DataFrame is empty")
        return {'loaded_count': 0}
    
    try:
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        
        truncate_sql = f"TRUNCATE TABLE {TARGET_TABLE}"
        oracle_hook.run(truncate_sql)
        context['task_instance'].log.info(f"Table {TARGET_TABLE} truncated successfully")
        
        insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            FI_INSTRUMENT_ID, FI_IBALTYPE_CD, ACCOUNT, SEQUENCENO, 
            PF_TRANS_DT, HBC_RECEIPT_NO, FI_BALANCE_AMT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7
        )
        """
        
        insert_data = []
        for _, row in df.iterrows():
            insert_data.append((
                row['FI_INSTRUMENT_ID'],
                row['FI_IBALTYPE_CD'],
                row['ACCOUNT'],
                int(row['SEQUENCENO']),
                row['PF_TRANS_DT'],
                row['HBC_RECEIPT_NO'],
                float(row['FI_BALANCE_AMT'])
            ))
        
        oracle_hook.bulk_insert_rows(
            table=TARGET_TABLE,
            rows=insert_data,
            target_fields=[
                'FI_INSTRUMENT_ID', 'FI_IBALTYPE_CD', 'ACCOUNT', 'SEQUENCENO',
                'PF_TRANS_DT', 'HBC_RECEIPT_NO', 'FI_BALANCE_AMT'
            ],
            commit_every=1000
        )
        
        loaded_count = len(insert_data)
        context['task_instance'].log.info(f"Successfully loaded {loaded_count} records into {TARGET_TABLE}")
        
        return {
            'loaded_count': loaded_count,
            'target_table': TARGET_TABLE
        }
        
    except Exception as e:
        raise AirflowException(f"Failed to load data into target table: {str(e)}")

validate_file_task = PythonOperator(
    task_id='validate_source_file',
    python_callable=validate_source_file,
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_source_data',
    python_callable=extract_source_data,
    dag=dag,
)

filter_data_task = PythonOperator(
    task_id='filter_null_receipts',
    python_callable=filter_null_receipts,
    dag=dag,
)

lookup_data_task = PythonOperator(
    task_id='lookup_period_control',
    python_callable=lookup_period_control,
    dag=dag,
)

transform_and_validate_task = PythonOperator(
    task_id='validate_dates_and_transform',
    python_callable=validate_dates_and_transform,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_target_data',
    python_callable=load_target_data,
    dag=dag,
)

validate_file_task >> extract_data_task >> filter_data_task
filter_data_task >> lookup_data_task
[filter_data_task, lookup_data_task] >> transform_and_validate_task >> load_data_task
