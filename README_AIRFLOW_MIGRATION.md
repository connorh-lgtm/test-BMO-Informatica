# BMO Financial Balance ETL - Airflow Migration

## Overview

This document describes the migration of the BMO Financial Balance ETL pipeline from Informatica PowerCenter to Apache Airflow. The migration preserves all business logic, transformations, and critical error handling from the original implementation.

## Migration Summary

### Original Informatica PowerCenter Implementation
- **Mapping**: `m_BMO_Balance_ETL`
- **Source**: Flat file `mrp0455_inv_gl.txt` (fixed-width format)
- **Target**: Oracle table `PS_HBC_INV_BAL_STG`
- **Lookup**: `PS_HBC_PERIOD_CTL` table for date validation
- **Critical Logic**: Date validation with session abort on mismatch

### New Airflow Implementation
- **DAG**: `bmo_balance_etl`
- **File**: `bmo_balance_etl_dag.py`
- **Schedule**: Daily execution
- **Tasks**: 6 tasks replicating the original data flow

## Data Flow Comparison

### Informatica PowerCenter Flow
```
Source Qualifier → Filter → Expression (Build Lookups) → Lookup → Expression (Date Validation & Field Construction) → Target
                                                                    ↑
                                                            Sequence Generator
```

### Airflow DAG Flow
```
validate_source_file → extract_source_data → filter_null_receipts → validate_dates_and_transform → load_target_data
                                                    ↓                           ↑
                                              lookup_period_control ────────────┘
```

## Task Details

### 1. validate_source_file
- **Purpose**: Validates existence and accessibility of source file
- **Equivalent**: Informatica session file validation
- **Error Handling**: Fails DAG if file not found or not readable

### 2. extract_source_data
- **Purpose**: Reads fixed-width flat file and parses fields
- **Equivalent**: Source Qualifier `SQ_mrp0455_inv_gl`
- **Fields**: Effective_Date, Receipt_Number, Balance_Type, Amount, Account
- **Format**: Fixed-width with specific column positions

### 3. filter_null_receipts
- **Purpose**: Removes records with NULL receipt numbers
- **Equivalent**: Filter transformation `FIL_Remove_Null_Receipts`
- **Condition**: `NOT ISNULL(Receipt_Number)`

### 4. lookup_period_control
- **Purpose**: Retrieves HBC_DATA_DATE from period control table
- **Equivalent**: Lookup transformation `LKP_HBC_PERIOD_CTL`
- **SQL**: `SELECT HBC_DATA_DATE FROM PS_HBC_PERIOD_CTL WHERE RTRIM(HBC_PER_CTL) = 'APS'`

### 5. validate_dates_and_transform
- **Purpose**: Critical date validation and field transformations
- **Equivalent**: Expression transformation `EXP_Check_Date_Build_Table`
- **Critical Logic**: 
  - Date validation with DAG failure on mismatch (equivalent to ABORT())
  - Field construction and NULL handling
  - Sequence number generation

### 6. load_target_data
- **Purpose**: Loads transformed data into target table
- **Equivalent**: Target definition `PS_HBC_INV_BAL_STG`
- **Options**: Table truncation before load

## Field Transformations

All field transformations from the original Informatica expressions are preserved:

| Target Field | Transformation Logic |
|--------------|---------------------|
| `FI_INSTRUMENT_ID` | `'INV' + Receipt_Number.zfill(42)` |
| `FI_IBALTYPE_CD` | `' ' if NULL else '\n' + Balance_Type.upper()` |
| `ACCOUNT` | `' ' if NULL else Account` |
| `SEQUENCENO` | Global sequence counter (1, 2, 3, ...) |
| `PF_TRANS_DT` | HBC_DATA_DATE from lookup |
| `HBC_RECEIPT_NO` | `' ' if NULL else '\n' + Receipt_Number` |
| `FI_BALANCE_AMT` | `0.000 if NULL else Decimal(Amount, 3)` |

## Critical Business Rules Preserved

### 1. Date Validation Abort Logic
- **Original**: `ABORT('Intrader Effective Date does not equal the HBC_DATA_DATE on the APS Period Control row')`
- **Airflow**: `raise AirflowException(...)` - Fails entire DAG execution
- **Trigger**: When `Effective_Date != HBC_DATA_DATE`

### 2. Field Padding
- **FI_INSTRUMENT_ID**: Exactly 42 digits with leading zeros
- **Original**: `LPAD(Receipt_Number, 42, '0')`
- **Airflow**: `str.zfill(42)`

### 3. NULL Handling
- Consistent space defaults for NULL values
- Newline character prefixes where specified
- Decimal precision maintained (3 decimal places)

## Configuration Requirements

### Airflow Connections
1. **Oracle Database Connection**
   - Connection ID: `oracle_bmo_db`
   - Type: Oracle
   - Host: [Oracle server]
   - Schema: [Database schema]
   - Login/Password: [Credentials]

### File System Access
- Source file path: `/data/input/mrp0455_inv_gl.txt`
- Ensure Airflow has read access to the file location

### DAG Configuration
- **Schedule**: `@daily` (can be adjusted)
- **Retries**: 1 with 5-minute delay
- **Timeout**: 2 hours
- **Email Notifications**: Enabled for failures

## Error Handling

### Informatica vs Airflow Error Handling

| Scenario | Informatica | Airflow |
|----------|-------------|---------|
| File not found | Session fails | DAG fails at validate_source_file |
| Date mismatch | ABORT() - Session stops | AirflowException - DAG fails |
| Database error | Session fails | Task fails with retry |
| Transformation error | Session fails | Task fails with logging |

## Testing Strategy

### Unit Testing
1. **File Validation**: Test with missing/empty files
2. **Date Validation**: Test with mismatched dates (should fail)
3. **Field Transformations**: Verify padding, NULL handling, case conversion
4. **Sequence Generation**: Verify proper incrementing

### Integration Testing
1. **End-to-End**: Full pipeline with sample data
2. **Database Connectivity**: Oracle connection and table access
3. **Error Scenarios**: Verify proper failure handling

## Deployment Steps

1. **Install Dependencies**
   ```bash
   pip install apache-airflow[oracle]
   pip install pandas numpy
   ```

2. **Configure Connections**
   - Set up Oracle connection in Airflow UI
   - Test database connectivity

3. **Deploy DAG**
   - Copy `bmo_balance_etl_dag.py` to Airflow DAGs folder
   - Verify DAG appears in Airflow UI

4. **Test Execution**
   - Run DAG with test data
   - Verify all transformations work correctly
   - Test error scenarios

## Monitoring and Maintenance

### Airflow Advantages
- **Web UI**: Visual DAG monitoring and task status
- **Logging**: Detailed task-level logging
- **Alerting**: Email notifications and custom alerts
- **Retry Logic**: Configurable retry policies
- **Scalability**: Distributed task execution

### Maintenance Tasks
- Monitor DAG execution times
- Review task failure patterns
- Update connection credentials as needed
- Adjust retry policies based on operational experience

## Performance Considerations

### Optimizations Applied
- **Bulk Insert**: Using Oracle bulk insert for target loading
- **Pandas Operations**: Vectorized transformations for better performance
- **Connection Pooling**: Reusing database connections
- **Memory Management**: Processing data in chunks if needed

### Monitoring Points
- File processing time
- Database lookup performance
- Transformation execution time
- Target table load performance

This migration maintains full fidelity to the original Informatica PowerCenter implementation while leveraging Airflow's modern orchestration capabilities.
