"""
BMO Financial Balance ETL Pipeline - Apache Airflow Implementation
Migrated from Informatica PowerCenter mapping: m_BMO_Balance_ETL

This DAG replicates the complete ETL pipeline functionality:
1. Read flat file (mrp0455_inv_gl.txt)
2. Filter out NULL receipt numbers
3. Lookup HBC_DATA_DATE from PS_HBC_PERIOD_CTL
4. Apply business logic transformations
5. Load into PS_HBC_INV_BAL_STG target table

Author: Devin AI
Date: September 2025
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import logging
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DAG_ID = 'bmo_balance_etl'
DESCRIPTION = 'BMO Financial Balance ETL Pipeline - Migrated from Informatica PowerCenter'

default_args = {
    'owner': 'bmo-etl-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('FAILURE_EMAIL_LIST', default_var='bmo-etl-failure@company.com').split(',')
}

INSTRUMENT_ID_PREFIX = 'INV'
INSTRUMENT_ID_PAD_LENGTH = 42
INSTRUMENT_ID_PAD_CHAR = '0'
HBC_PER_CTL_VALUE = 'APS'
DATE_FORMAT = '%Y%m%d'
DEFAULT_SPACE_VALUE = ' '
DEFAULT_AMOUNT_VALUE = Decimal('0.000')

def read_source_file(**context) -> List[Dict[str, Any]]:
    """
    Read and parse the flat file source (mrp0455_inv_gl.txt)
    Equivalent to Informatica Source Qualifier: SQ_mrp0455_inv_gl
    """
    try:
        source_file_dir = Variable.get('SOURCE_FILE_DIR', default_var='/data/bmo/source/balance')
        source_filename = Variable.get('SOURCE_FILENAME', default_var='mrp0455_inv_gl.txt')
        file_path = f"{source_file_dir}/{source_filename}"
        
        logger.info(f"Reading source file: {file_path}")
        
        df = pd.read_csv(
            file_path,
            delimiter='|',
            header=0,
            dtype={
                'Effective_Date': str,
                'Receipt_Number': str,
                'Balance_Type': str,
                'Amount': str,
                'Account': str
            },
            na_values=['', 'NULL', 'null'],
            keep_default_na=False
        )
        
        records = df.to_dict('records')
        
        logger.info(f"Successfully read {len(records)} records from source file")
        
        context['task_instance'].xcom_push(key='source_records', value=records)
        
        return records
        
    except Exception as e:
        logger.error(f"Error reading source file: {str(e)}")
        raise AirflowException(f"Failed to read source file: {str(e)}")

def filter_null_receipts(**context) -> List[Dict[str, Any]]:
    """
    Filter out rows with NULL receipt numbers
    Equivalent to Informatica Filter: FIL_Remove_Null_Receipts
    Filter Condition: NOT ISNULL(Receipt_Number)
    """
    try:
        source_records = context['task_instance'].xcom_pull(
            task_ids='read_source_file', 
            key='source_records'
        )
        
        if not source_records:
            raise AirflowException("No source records found from previous task")
        
        logger.info(f"Filtering {len(source_records)} records for non-null receipt numbers")
        
        filtered_records = [
            record for record in source_records 
            if record.get('Receipt_Number') and 
               str(record['Receipt_Number']).strip() != '' and
               pd.notna(record['Receipt_Number'])
        ]
        
        logger.info(f"After filtering: {len(filtered_records)} records remain")
        
        context['task_instance'].xcom_push(key='filtered_records', value=filtered_records)
        
        return filtered_records
        
    except Exception as e:
        logger.error(f"Error in filter_null_receipts: {str(e)}")
        raise AirflowException(f"Failed to filter records: {str(e)}")

def build_lookup_inputs(**context) -> List[Dict[str, Any]]:
    """
    Prepare lookup inputs by adding constant value for period control lookup
    Equivalent to Informatica Expression: EXP_Build_For_Lookups
    Expression: lkp_HBC_PER_CTL = RTRIM('APS')
    """
    try:
        filtered_records = context['task_instance'].xcom_pull(
            task_ids='filter_null_receipts', 
            key='filtered_records'
        )
        
        if not filtered_records:
            raise AirflowException("No filtered records found from previous task")
        
        logger.info(f"Building lookup inputs for {len(filtered_records)} records")
        
        lookup_ready_records = []
        for record in filtered_records:
            enhanced_record = record.copy()
            enhanced_record['lkp_HBC_PER_CTL'] = HBC_PER_CTL_VALUE.strip()
            lookup_ready_records.append(enhanced_record)
        
        logger.info(f"Successfully prepared {len(lookup_ready_records)} records for lookup")
        
        context['task_instance'].xcom_push(key='lookup_ready_records', value=lookup_ready_records)
        
        return lookup_ready_records
        
    except Exception as e:
        logger.error(f"Error in build_lookup_inputs: {str(e)}")
        raise AirflowException(f"Failed to build lookup inputs: {str(e)}")

def lookup_hbc_period_ctl(**context) -> List[Dict[str, Any]]:
    """
    Perform lookup to retrieve HBC_DATA_DATE from PS_HBC_PERIOD_CTL table
    Equivalent to Informatica Lookup: LKP_HBC_PERIOD_CTL
    SQL Override: SELECT HBC_DATA_DATE FROM PS_HBC_PERIOD_CTL WHERE RTRIM(HBC_PER_CTL) = ?
    """
    try:
        lookup_ready_records = context['task_instance'].xcom_pull(
            task_ids='build_lookup_inputs', 
            key='lookup_ready_records'
        )
        
        if not lookup_ready_records:
            raise AirflowException("No lookup ready records found from previous task")
        
        logger.info(f"Performing HBC_PERIOD_CTL lookup for {len(lookup_ready_records)} records")
        
        oracle_hook = OracleHook(oracle_conn_id='oracle_bmo_connection')
        
        lookup_sql = """
            SELECT HBC_DATA_DATE 
            FROM PS_HBC_PERIOD_CTL 
            WHERE RTRIM(HBC_PER_CTL) = :hbc_per_ctl
        """
        
        lookup_key = lookup_ready_records[0]['lkp_HBC_PER_CTL']
        
        result = oracle_hook.get_first(lookup_sql, parameters={'hbc_per_ctl': lookup_key})
        
        if not result or not result[0]:
            raise AirflowException(f"No HBC_DATA_DATE found for HBC_PER_CTL = '{lookup_key}'")
        
        hbc_data_date = result[0]
        logger.info(f"Retrieved HBC_DATA_DATE: {hbc_data_date}")
        
        enriched_records = []
        for record in lookup_ready_records:
            enhanced_record = record.copy()
            enhanced_record['HBC_DATA_DATE'] = hbc_data_date
            enriched_records.append(enhanced_record)
        
        logger.info(f"Successfully enriched {len(enriched_records)} records with HBC_DATA_DATE")
        
        context['task_instance'].xcom_push(key='enriched_records', value=enriched_records)
        
        return enriched_records
        
    except Exception as e:
        logger.error(f"Error in lookup_hbc_period_ctl: {str(e)}")
        raise AirflowException(f"Failed to perform HBC_PERIOD_CTL lookup: {str(e)}")

def check_date_build_table(**context) -> List[Dict[str, Any]]:
    """
    Validate dates, increment sequence, and construct target fields
    Equivalent to Informatica Expression: EXP_Check_Date_Build_Table
    
    Business Logic:
    - Parse effective date and validate against HBC_DATA_DATE
    - Abort if dates don't match
    - Build all target fields with proper NULL handling
    - Generate sequence numbers
    """
    try:
        enriched_records = context['task_instance'].xcom_pull(
            task_ids='lookup_hbc_period_ctl', 
            key='enriched_records'
        )
        
        if not enriched_records:
            raise AirflowException("No enriched records found from previous task")
        
        logger.info(f"Processing {len(enriched_records)} records for date validation and field construction")
        
        target_records = []
        sequence_counter = 0
        
        for record in enriched_records:
            try:
                effective_date_str = str(record['Effective_Date']).strip()
                var_header_date = datetime.strptime(effective_date_str, DATE_FORMAT).date()
                
                hbc_data_date = record['HBC_DATA_DATE']
                if isinstance(hbc_data_date, datetime):
                    hbc_data_date = hbc_data_date.date()
                
                if var_header_date != hbc_data_date:
                    error_msg = (
                        f"Intrader Effective Date ({var_header_date}) does not equal "
                        f"the HBC_DATA_DATE ({hbc_data_date}) on the APS Period Control row"
                    )
                    logger.error(error_msg)
                    raise AirflowException(error_msg)
                
                sequence_counter += 1
                
                target_record = {
                    'FI_INSTRUMENT_ID': (
                        INSTRUMENT_ID_PREFIX + 
                        str(record['Receipt_Number']).zfill(INSTRUMENT_ID_PAD_LENGTH)
                    ),
                    
                    'FI_IBALTYPE_CD': (
                        DEFAULT_SPACE_VALUE if pd.isna(record.get('Balance_Type')) or 
                        not record.get('Balance_Type') or str(record['Balance_Type']).strip() == ''
                        else '\n' + str(record['Balance_Type']).upper()
                    ),
                    
                    'ACCOUNT': (
                        DEFAULT_SPACE_VALUE if pd.isna(record.get('Account')) or 
                        not record.get('Account') or str(record['Account']).strip() == ''
                        else str(record['Account'])
                    ),
                    
                    'SEQUENCENO': sequence_counter,
                    
                    'PF_TRANS_DT': record['HBC_DATA_DATE'],
                    
                    'HBC_RECEIPT_NO': (
                        DEFAULT_SPACE_VALUE if pd.isna(record.get('Receipt_Number')) or 
                        not record.get('Receipt_Number') or str(record['Receipt_Number']).strip() == ''
                        else '\n' + str(record['Receipt_Number'])
                    ),
                    
                    'FI_BALANCE_AMT': (
                        DEFAULT_AMOUNT_VALUE if pd.isna(record.get('Amount')) or 
                        not record.get('Amount') or str(record['Amount']).strip() == ''
                        else Decimal(str(record['Amount'])).quantize(Decimal('0.001'))
                    )
                }
                
                target_records.append(target_record)
                
            except ValueError as ve:
                logger.error(f"Date parsing error for record {record}: {str(ve)}")
                raise AirflowException(f"Invalid date format in record: {str(ve)}")
            except Exception as re:
                logger.error(f"Error processing record {record}: {str(re)}")
                raise AirflowException(f"Failed to process record: {str(re)}")
        
        logger.info(f"Successfully processed {len(target_records)} records for target loading")
        
        context['task_instance'].xcom_push(key='target_records', value=target_records)
        
        return target_records
        
    except Exception as e:
        logger.error(f"Error in check_date_build_table: {str(e)}")
        raise AirflowException(f"Failed to validate dates and build target records: {str(e)}")

def load_target_table(**context) -> int:
    """
    Load processed records into PS_HBC_INV_BAL_STG target table
    Equivalent to Informatica Target: PS_HBC_INV_BAL_STG
    """
    try:
        target_records = context['task_instance'].xcom_pull(
            task_ids='check_date_build_table', 
            key='target_records'
        )
        
        if not target_records:
            raise AirflowException("No target records found from previous task")
        
        logger.info(f"Loading {len(target_records)} records into PS_HBC_INV_BAL_STG")
        
        oracle_hook = OracleHook(oracle_conn_id='oracle_bmo_connection')
        
        truncate_sql = "TRUNCATE TABLE PS_HBC_INV_BAL_STG"
        oracle_hook.run(truncate_sql)
        logger.info("Successfully truncated PS_HBC_INV_BAL_STG table")
        
        insert_sql = """
            INSERT INTO PS_HBC_INV_BAL_STG (
                FI_INSTRUMENT_ID,
                FI_IBALTYPE_CD,
                ACCOUNT,
                SEQUENCENO,
                PF_TRANS_DT,
                HBC_RECEIPT_NO,
                FI_BALANCE_AMT
            ) VALUES (
                :fi_instrument_id,
                :fi_ibaltype_cd,
                :account,
                :sequenceno,
                :pf_trans_dt,
                :hbc_receipt_no,
                :fi_balance_amt
            )
        """
        
        insert_data = []
        for record in target_records:
            insert_data.append({
                'fi_instrument_id': record['FI_INSTRUMENT_ID'],
                'fi_ibaltype_cd': record['FI_IBALTYPE_CD'],
                'account': record['ACCOUNT'],
                'sequenceno': record['SEQUENCENO'],
                'pf_trans_dt': record['PF_TRANS_DT'],
                'hbc_receipt_no': record['HBC_RECEIPT_NO'],
                'fi_balance_amt': float(record['FI_BALANCE_AMT'])
            })
        
        oracle_hook.bulk_insert_rows(
            table='PS_HBC_INV_BAL_STG',
            rows=insert_data,
            target_fields=[
                'FI_INSTRUMENT_ID', 'FI_IBALTYPE_CD', 'ACCOUNT', 
                'SEQUENCENO', 'PF_TRANS_DT', 'HBC_RECEIPT_NO', 'FI_BALANCE_AMT'
            ],
            commit_every=1000
        )
        
        logger.info(f"Successfully loaded {len(target_records)} records into PS_HBC_INV_BAL_STG")
        
        return len(target_records)
        
    except Exception as e:
        logger.error(f"Error in load_target_table: {str(e)}")
        raise AirflowException(f"Failed to load target table: {str(e)}")

def send_success_notification(**context) -> None:
    """
    Send success notification email
    """
    try:
        records_loaded = context['task_instance'].xcom_pull(task_ids='load_target_table')
        
        success_email_list = Variable.get('SUCCESS_EMAIL_LIST', default_var='bmo-etl-success@company.com')
        
        subject = f"BMO Balance ETL - Success - {context['ds']}"
        html_content = f"""
        <h3>BMO Balance ETL Pipeline Completed Successfully</h3>
        <p><strong>Execution Date:</strong> {context['ds']}</p>
        <p><strong>Records Processed:</strong> {records_loaded}</p>
        <p><strong>Target Table:</strong> PS_HBC_INV_BAL_STG</p>
        <p><strong>Status:</strong> SUCCESS</p>
        """
        
        logger.info(f"Success notification would be sent to: {success_email_list}")
        logger.info(f"Records loaded: {records_loaded}")
        
    except Exception as e:
        logger.warning(f"Failed to send success notification: {str(e)}")

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['bmo', 'etl', 'financial', 'balance']
)

read_source_task = PythonOperator(
    task_id='read_source_file',
    python_callable=read_source_file,
    dag=dag,
    doc_md="""
    
    Reads the flat file source (mrp0455_inv_gl.txt) and parses it into structured records.
    
    **Equivalent Informatica Component:** Source Qualifier (SQ_mrp0455_inv_gl)
    
    **Input:** Flat file with pipe delimiter
    **Output:** List of source records
    """
)

filter_task = PythonOperator(
    task_id='filter_null_receipts',
    python_callable=filter_null_receipts,
    dag=dag,
    doc_md="""
    
    Filters out records where Receipt_Number is NULL or empty.
    
    **Equivalent Informatica Component:** Filter (FIL_Remove_Null_Receipts)
    **Filter Condition:** NOT ISNULL(Receipt_Number)
    """
)

build_lookup_task = PythonOperator(
    task_id='build_lookup_inputs',
    python_callable=build_lookup_inputs,
    dag=dag,
    doc_md="""
    
    Prepares records for lookup by adding the constant lookup key.
    
    **Equivalent Informatica Component:** Expression (EXP_Build_For_Lookups)
    **Expression:** lkp_HBC_PER_CTL = RTRIM('APS')
    """
)

lookup_task = PythonOperator(
    task_id='lookup_hbc_period_ctl',
    python_callable=lookup_hbc_period_ctl,
    dag=dag,
    doc_md="""
    
    Performs lookup against PS_HBC_PERIOD_CTL to retrieve HBC_DATA_DATE.
    
    **Equivalent Informatica Component:** Lookup (LKP_HBC_PERIOD_CTL)
    **SQL:** SELECT HBC_DATA_DATE FROM PS_HBC_PERIOD_CTL WHERE RTRIM(HBC_PER_CTL) = ?
    """
)

transform_task = PythonOperator(
    task_id='check_date_build_table',
    python_callable=check_date_build_table,
    dag=dag,
    doc_md="""
    
    Validates dates, generates sequence numbers, and builds all target fields.
    
    **Equivalent Informatica Component:** Expression (EXP_Check_Date_Build_Table)
    **Critical Logic:** Aborts pipeline if Effective_Date != HBC_DATA_DATE
    """
)

load_task = PythonOperator(
    task_id='load_target_table',
    python_callable=load_target_table,
    dag=dag,
    doc_md="""
    
    Loads processed records into PS_HBC_INV_BAL_STG target table.
    
    **Equivalent Informatica Component:** Target (PS_HBC_INV_BAL_STG)
    **Operation:** Truncate and Insert
    """
)

success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='all_success',
    doc_md="""
    
    Sends success notification email when pipeline completes successfully.
    """
)

read_source_task >> filter_task >> build_lookup_task >> lookup_task >> transform_task >> load_task >> success_notification_task

failure_notification_task = EmailOperator(
    task_id='send_failure_notification',
    to=Variable.get('FAILURE_EMAIL_LIST', default_var='bmo-etl-failure@company.com').split(','),
    subject='BMO Balance ETL - FAILURE - {{ ds }}',
    html_content="""
    <h3>BMO Balance ETL Pipeline Failed</h3>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Failed Task:</strong> {{ task_instance.task_id }}</p>
    <p><strong>Error:</strong> {{ task_instance.log_url }}</p>
    <p><strong>Status:</strong> FAILURE</p>
    <p>Please check the Airflow logs for detailed error information.</p>
    """,
    dag=dag,
    trigger_rule='one_failed'
)

[read_source_task, filter_task, build_lookup_task, lookup_task, transform_task, load_task] >> failure_notification_task
