# BMO Balance ETL - Apache Airflow Implementation

## Overview

This directory contains the Apache Airflow implementation of the BMO Financial Balance ETL pipeline, migrated from the Informatica PowerCenter mapping `m_BMO_Balance_ETL`.

## Architecture

The Airflow DAG replicates the exact same data flow and business logic as the original Informatica implementation:

```
Source File (mrp0455_inv_gl.txt)
    ↓
Filter (Remove NULL Receipt Numbers)
    ↓
Build Lookup Inputs (Add APS constant)
    ↓
Lookup (PS_HBC_PERIOD_CTL → HBC_DATA_DATE)
    ↓
Transform (Date validation + Field construction)
    ↓
Load (PS_HBC_INV_BAL_STG)
    ↓
Success Notification
```

## Files Structure

```
airflow/
├── dags/
│   └── bmo_balance_etl_dag.py          # Main DAG implementation
├── config/
│   ├── airflow_variables.json         # Airflow Variables configuration
│   └── connections.json               # Database connections configuration
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## Key Features

### 🔄 **Complete Transformation Logic Migration**
- **Source Qualifier**: Reads pipe-delimited flat file with header skip
- **Filter**: Removes records with NULL/empty Receipt_Number
- **Expression**: Builds lookup inputs with APS constant
- **Lookup**: Retrieves HBC_DATA_DATE from PS_HBC_PERIOD_CTL
- **Expression**: Date validation, sequence generation, field construction
- **Target**: Bulk insert into PS_HBC_INV_BAL_STG with truncate

### 🛡️ **Business Logic Preservation**
- **Date Validation**: Pipeline aborts if Effective_Date ≠ HBC_DATA_DATE
- **Field Mappings**: Exact replication of Informatica expressions
- **NULL Handling**: Proper space/zero defaults for NULL values
- **Sequence Generation**: Row counter equivalent to Informatica NEXTVAL

### 📧 **Error Handling & Notifications**
- **Success Notifications**: Email on successful completion
- **Failure Notifications**: Email on any task failure
- **Logging**: Comprehensive logging at each transformation step
- **Retry Logic**: Configurable retry attempts with delays

### ⚙️ **Configuration Management**
- **Environment Variables**: Secure credential management
- **Airflow Variables**: Runtime configuration parameters
- **Connections**: Database connection pooling
- **Parameterization**: Environment-specific overrides

## Installation & Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Airflow Variables
```bash
# Import variables from JSON
airflow variables import config/airflow_variables.json
```

### 3. Set Up Database Connection
```bash
# Create Oracle connection
airflow connections add oracle_bmo_connection \
    --conn-type oracle \
    --conn-host ${DB_HOST} \
    --conn-port ${DB_PORT} \
    --conn-schema BMO_FINANCIAL \
    --conn-login ${DB_USERNAME} \
    --conn-password ${DB_PASSWORD} \
    --conn-extra '{"service_name": "${DB_SERVICE_NAME}"}'
```

### 4. Environment Variables
Set the following environment variables (use the template from `deployment/environment_variables.template`):

```bash
export DB_HOST=your-oracle-host
export DB_PORT=1521
export DB_USERNAME=BMO_FINANCIAL_USER
export DB_PASSWORD=your-secure-password
export DB_SERVICE_NAME=BMOPROD
```

### 5. Deploy DAG
```bash
# Copy DAG to Airflow DAGs folder
cp dags/bmo_balance_etl_dag.py $AIRFLOW_HOME/dags/
```

## Usage

### Manual Execution
```bash
# Trigger DAG run
airflow dags trigger bmo_balance_etl

# Monitor execution
airflow dags state bmo_balance_etl 2025-09-17
```

### Scheduled Execution
The DAG is configured to run daily (`@daily` schedule). Modify the `schedule_interval` in the DAG file for different schedules.

## Monitoring & Troubleshooting

### Task Status Monitoring
```bash
# Check DAG status
airflow dags list-runs -d bmo_balance_etl

# View task logs
airflow tasks log bmo_balance_etl read_source_file 2025-09-17
```

### Common Issues

1. **File Not Found**
   - Verify `SOURCE_FILE_DIR` variable points to correct directory
   - Check file permissions and existence

2. **Database Connection Issues**
   - Test connection: `airflow connections test oracle_bmo_connection`
   - Verify environment variables are set correctly

3. **Date Validation Failures**
   - Check PS_HBC_PERIOD_CTL has correct APS record
   - Verify source file date format (YYYYMMDD)

## Performance Considerations

- **Bulk Operations**: Uses bulk insert for target loading
- **Connection Pooling**: Oracle connection pooling enabled
- **Memory Management**: Pandas operations optimized for large datasets
- **Parallel Processing**: Tasks run in sequence but can be parallelized if needed

## Migration Notes

### Differences from Informatica
1. **Sequence Generation**: Uses Python counter instead of database sequence
2. **Caching**: No persistent lookup caching (queries database each run)
3. **Error Handling**: Python exceptions instead of Informatica ABORT function
4. **Logging**: Airflow task logs instead of PowerCenter session logs

### Equivalent Mappings
| Informatica Component | Airflow Task | Function |
|----------------------|--------------|----------|
| SQ_mrp0455_inv_gl | read_source_file | Source file reading |
| FIL_Remove_Null_Receipts | filter_null_receipts | NULL filtering |
| EXP_Build_For_Lookups | build_lookup_inputs | Lookup preparation |
| LKP_HBC_PERIOD_CTL | lookup_hbc_period_ctl | Database lookup |
| EXP_Check_Date_Build_Table | check_date_build_table | Business logic |
| PS_HBC_INV_BAL_STG | load_target_table | Target loading |

## Support

For issues or questions:
- **ETL Team**: ${ETL_SUPPORT_EMAIL}
- **Database Team**: ${DBA_SUPPORT_EMAIL}
- **Infrastructure**: ${INFRA_SUPPORT_EMAIL}
- **On-Call**: ${ONCALL_SUPPORT_PHONE}

---

**Migration Date**: September 2025  
**Original Implementation**: Informatica PowerCenter 9.6.1  
**Airflow Version**: 2.7.0
