# BMO Balance ETL - Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying the BMO Financial Balance ETL pipeline in Informatica PowerCenter environments.

## Prerequisites

### System Requirements
- Informatica PowerCenter 9.6.1 or higher
- Oracle Database 11g or higher
- Unix/Linux server for PowerCenter services
- Minimum 4GB RAM allocated to Integration Service
- Sufficient disk space for source files, logs, and cache

### Access Requirements
- PowerCenter Repository Administrator access
- Oracle database connection with INSERT privileges on `PS_HBC_INV_BAL_STG`
- Oracle database connection with SELECT privileges on `PS_HBC_PERIOD_CTL`
- File system access to source file directories
- SMTP server access for email notifications (optional)

## Deployment Steps

### 1. Repository Setup

#### Import Source and Target Definitions
```bash
# Import source definition
pmrep importobjects -i source_definitions/SRC_mrp0455_inv_gl.xml -c

# Import target definition  
pmrep importobjects -i target_definitions/TGT_PS_HBC_INV_BAL_STG.xml -c
```

#### Import Mapping
```bash
# Import the main mapping
pmrep importobjects -i Informatica_PowerCenter_Mapping.xml -c
```

### 2. Connection Configuration

#### Create Source Connection (Flat File)
- **Connection Name**: `FF_BMO_Source`
- **Connection Type**: FlatFile
- **Connection String**: `/data/bmo/source/balance`

#### Create Target Connection (Oracle)
- **Connection Name**: `DB_BMO_Oracle`
- **Connection Type**: Oracle
- **Connection String**: `${DB_USERNAME}/${DB_PASSWORD}@${DB_HOST}:${DB_PORT}:${DB_SERVICE_NAME}`

### 3. Session and Workflow Import

#### Import Session Configuration
```bash
# Import session
pmrep importobjects -i sessions/s_m_BMO_Balance_ETL.xml -c
```

#### Import Workflow
```bash
# Import workflow
pmrep importobjects -i workflows/wf_BMO_Balance_ETL.xml -c
```

### 4. Parameter File Configuration

#### Environment-Specific Parameter Files

**Development Environment** (`BMO_Balance_ETL_DEV.param`):
```properties
$PMSourceFileDir=/data/bmo/dev/source/balance
$PMTargetConnectionString=${DEV_DB_USERNAME}/${DEV_DB_PASSWORD}@${DEV_DB_HOST}:${DEV_DB_PORT}:${DEV_DB_SERVICE}
$PMSessionLogDir=/data/bmo/dev/logs/sessions
$$CommitInterval=1000
$$StopOnErrors=NO
```

**Production Environment** (`BMO_Balance_ETL_PROD.param`):
```properties
$PMSourceFileDir=/data/bmo/prod/source/balance
$PMTargetConnectionString=${PROD_DB_USERNAME}/${PROD_DB_PASSWORD}@${PROD_DB_HOST}:${PROD_DB_PORT}:${PROD_DB_SERVICE}
$PMSessionLogDir=/data/bmo/prod/logs/sessions
$$CommitInterval=10000
$$StopOnErrors=YES
```

### 5. Directory Structure Setup

Create the following directory structure on the PowerCenter server:

```
/data/bmo/
├── source/
│   └── balance/           # Source files location
├── logs/
│   ├── sessions/          # Session log files
│   └── workflows/         # Workflow log files
├── reject/                # Reject/bad files
├── cache/                 # Lookup cache files
└── archive/               # Processed file archive
```

#### Set Permissions
```bash
# Set ownership and permissions
chown -R informatica:informatica /data/bmo/
chmod -R 755 /data/bmo/
chmod -R 777 /data/bmo/logs/
chmod -R 777 /data/bmo/reject/
chmod -R 777 /data/bmo/cache/
```

### 6. Database Setup

#### Create Target Table
```sql
-- Create staging table
CREATE TABLE PS_HBC_INV_BAL_STG (
    FI_INSTRUMENT_ID    VARCHAR2(45) NOT NULL,
    FI_IBALTYPE_CD      VARCHAR2(10),
    ACCOUNT             VARCHAR2(20),
    SEQUENCENO          NUMBER(10) NOT NULL,
    PF_TRANS_DT         DATE,
    HBC_RECEIPT_NO      VARCHAR2(20),
    FI_BALANCE_AMT      NUMBER(15,3)
);

-- Create indexes for performance
CREATE INDEX IDX_HBC_INV_BAL_STG_01 ON PS_HBC_INV_BAL_STG(FI_INSTRUMENT_ID);
CREATE INDEX IDX_HBC_INV_BAL_STG_02 ON PS_HBC_INV_BAL_STG(PF_TRANS_DT);
```

#### Verify Lookup Table
```sql
-- Verify PS_HBC_PERIOD_CTL exists and has data
SELECT HBC_PER_CTL, HBC_DATA_DATE 
FROM PS_HBC_PERIOD_CTL 
WHERE RTRIM(HBC_PER_CTL) = 'APS';
```

### 7. Testing and Validation

#### Unit Testing
1. **Source File Validation**
   ```bash
   # Test with sample file
   head -5 /data/bmo/source/balance/mrp0455_inv_gl.txt
   ```

2. **Connection Testing**
   ```bash
   # Test database connectivity
   pmcmd ping -service IntegrationService_BMO -domain Domain_BMO
   ```

3. **Mapping Validation**
   ```bash
   # Validate mapping
   pmcmd validatemapping -folder BMO_BALANCE_ETL -mapping m_BMO_Balance_ETL
   ```

#### Integration Testing
1. **Session Execution**
   ```bash
   # Run session with test data
   pmcmd startsession -folder BMO_BALANCE_ETL -session s_m_BMO_Balance_ETL -paramfile BMO_Balance_ETL_DEV.param
   ```

2. **Workflow Execution**
   ```bash
   # Run complete workflow
   pmcmd startworkflow -folder BMO_BALANCE_ETL -workflow wf_BMO_Balance_ETL -paramfile BMO_Balance_ETL_DEV.param
   ```

### 8. Monitoring and Maintenance

#### Log Monitoring
- **Session Logs**: `/data/bmo/logs/sessions/s_m_BMO_Balance_ETL.log`
- **Workflow Logs**: `/data/bmo/logs/workflows/wf_BMO_Balance_ETL.log`
- **Reject Files**: `/data/bmo/reject/mrp0455_inv_gl.bad`

#### Performance Monitoring
```bash
# Monitor session performance
pmcmd getsessionstatistics -folder BMO_BALANCE_ETL -session s_m_BMO_Balance_ETL
```

#### Maintenance Tasks
1. **Log Cleanup** (Weekly)
   ```bash
   find /data/bmo/logs -name "*.log" -mtime +7 -delete
   ```

2. **Cache Cleanup** (Daily)
   ```bash
   find /data/bmo/cache -name "*.idx" -mtime +1 -delete
   find /data/bmo/cache -name "*.dat" -mtime +1 -delete
   ```

3. **Archive Processed Files** (Daily)
   ```bash
   mv /data/bmo/source/balance/mrp0455_inv_gl.txt /data/bmo/archive/mrp0455_inv_gl_$(date +%Y%m%d).txt
   ```

## Troubleshooting

### Common Issues

#### 1. Date Validation Failure
**Error**: "Intrader Effective Date does not equal the HBC_DATA_DATE"
**Solution**: 
- Verify `PS_HBC_PERIOD_CTL` has correct date for 'APS' period
- Check source file date format (YYYYMMDD)

#### 2. File Not Found
**Error**: "Source file not found"
**Solution**:
- Verify file path in parameter file
- Check file permissions
- Ensure file naming convention matches

#### 3. Database Connection Issues
**Error**: "Connection failed"
**Solution**:
- Test database connectivity
- Verify connection string format
- Check database credentials

#### 4. Performance Issues
**Symptoms**: Slow session execution
**Solutions**:
- Increase DTM buffer size
- Enable lookup caching
- Optimize commit interval
- Add database indexes

## Security Considerations

### Password Management
- Use encrypted passwords in connection strings
- Store parameter files in secure locations
- Implement password rotation policies

### Access Control
- Limit repository access to authorized users
- Use role-based security for folder access
- Audit user activities regularly

### Data Protection
- Encrypt sensitive data in transit
- Implement data masking for non-production environments
- Secure reject files containing sensitive data

## Rollback Procedures

### Emergency Rollback
1. **Stop Running Sessions**
   ```bash
   pmcmd stopsession -folder BMO_BALANCE_ETL -session s_m_BMO_Balance_ETL
   ```

2. **Restore Previous Version**
   ```bash
   pmrep importobjects -i backup/previous_version.xml -c -replace
   ```

3. **Verify Rollback**
   ```bash
   pmcmd validatemapping -folder BMO_BALANCE_ETL -mapping m_BMO_Balance_ETL
   ```

## Support Contacts

- **ETL Team**: ${ETL_SUPPORT_EMAIL}
- **Database Team**: ${DBA_SUPPORT_EMAIL}
- **Infrastructure Team**: ${INFRA_SUPPORT_EMAIL}
- **On-Call Support**: ${ONCALL_SUPPORT_PHONE}

---

**Document Version**: 1.0  
**Last Updated**: September 2025  
**Next Review**: December 2025
