# Informatica PowerCenter Implementation Guide
## BMO Financial Balance ETL Pipeline

### Overview
This document provides the Informatica PowerCenter implementation for the financial balance ETL pipeline based on the pseudo-code in `pseudo_ETL_code.txt`. The implementation translates each pseudo-code component into corresponding PowerCenter transformations.

### Mapping Architecture

#### Data Flow Sequence
1. **Source Qualifier** → **Filter** → **Expression (Build Lookups)** → **Lookup** → **Expression (Date Validation & Field Construction)** → **Target**
2. **Sequence Generator** → **Expression (Date Validation & Field Construction)**

### Transformation Details

#### 1. Source Definition: `SQ_mrp0455_inv_gl`
- **Type**: Flat File Source
- **File**: `mrp0455_inv_gl.txt`
- **Fields**:
  - `Effective_Date` (String, 8 chars)
  - `Receipt_Number` (String, 20 chars)
  - `Balance_Type` (String, 10 chars)
  - `Amount` (Decimal, 15,3)
  - `Account` (String, 20 chars)

#### 2. Filter Transformation: `FIL_Remove_Null_Receipts`
- **Purpose**: Implements `filter_rows()` function from pseudo-code
- **Condition**: `NOT ISNULL(Receipt_Number)`
- **Logic**: Filters out records where Receipt_Number is NULL

#### 3. Expression Transformation: `EXP_Build_For_Lookups`
- **Purpose**: Implements `exp_build_for_lookups()` function
- **Key Logic**:
  - Passes through all source fields
  - Creates lookup input: `lkp_HBC_PER_CTL = RTRIM('APS')`

#### 4. Lookup Transformation: `LKP_HBC_PERIOD_CTL`
- **Purpose**: Implements `lookup_HBC_PERIOD_CTL()` function
- **Source Table**: `PS_HBC_PERIOD_CTL`
- **SQL Override**: 
  ```sql
  SELECT HBC_DATA_DATE 
  FROM PS_HBC_PERIOD_CTL 
  WHERE RTRIM(HBC_PER_CTL) = ?
  ```
- **Lookup Condition**: `lkp_HBC_PER_CTL`
- **Return Value**: `HBC_DATA_DATE`

#### 5. Sequence Generator: `SEQ_ROW_COUNT`
- **Purpose**: Implements global `ROW_COUNT` variable
- **Configuration**:
  - Start Value: 1
  - Increment By: 1
  - Cache Size: 1000
- **Output**: `NEXTVAL` (used as SEQUENCENO)

#### 6. Expression Transformation: `EXP_Check_Date_Build_Table`
- **Purpose**: Implements `exp_check_date_build_table()` function
- **Critical Business Logic**:

##### Date Validation with Abort Logic
```sql
-- Variable Port
var_Header_Date = TO_DATE(Effective_Date, 'YYYYMMDD')

-- Abort Logic
IIF(var_Header_Date != HBC_DATA_DATE, 
    ABORT('Intrader Effective Date does not equal the HBC_DATA_DATE on the APS Period Control row'), 
    NULL)
```

##### Field Transformations
```sql
-- FI_INSTRUMENT_ID: "INV" + left-padded Receipt_Number to 42 digits
FI_INSTRUMENT_ID = 'INV' || LPAD(Receipt_Number, 42, '0')

-- FI_IBALTYPE_CD: NULL handling with space default, uppercase conversion
FI_IBALTYPE_CD = IIF(ISNULL(Balance_Type), ' ', CHR(10) || UPPER(Balance_Type))

-- ACCOUNT: NULL handling with space default
ACCOUNT = IIF(ISNULL(Account), ' ', Account)

-- SEQUENCENO: Global sequence from Sequence Generator
SEQUENCENO = NEXTVAL

-- PF_TRANS_DT: Date from lookup
PF_TRANS_DT = HBC_DATA_DATE

-- HBC_RECEIPT_NO: NULL handling with space default
HBC_RECEIPT_NO = IIF(ISNULL(Receipt_Number), ' ', CHR(10) || Receipt_Number)

-- FI_BALANCE_AMT: Decimal conversion with NULL handling
FI_BALANCE_AMT = IIF(ISNULL(Amount), 0.000, TO_DECIMAL(Amount, 15, 3))
```

#### 7. Target Definition: `PS_HBC_INV_BAL_STG`
- **Type**: Oracle Database Table
- **Load Type**: Insert
- **Table Option**: Truncate Table (optional)
- **Fields**:
  - `FI_INSTRUMENT_ID` (String, 45 chars, NOT NULL)
  - `FI_IBALTYPE_CD` (String, 10 chars, NULLABLE)
  - `ACCOUNT` (String, 20 chars, NULLABLE)
  - `SEQUENCENO` (BigInt, NOT NULL)
  - `PF_TRANS_DT` (DateTime, NULLABLE)
  - `HBC_RECEIPT_NO` (String, 20 chars, NULLABLE)
  - `FI_BALANCE_AMT` (Decimal 15,3, NULLABLE)

### Key Implementation Notes

#### Critical Business Rules Preserved
1. **Date Validation Abort**: The mapping will abort if `Effective_Date` ≠ `HBC_DATA_DATE`
2. **Sequence Integrity**: Global `ROW_COUNT` maintained via Sequence Generator
3. **Field Padding**: `FI_INSTRUMENT_ID` properly padded to 42 digits with leading zeros
4. **NULL Handling**: Consistent NULL handling across all fields as per pseudo-code

#### PowerCenter-Specific Considerations
1. **Lookup Caching**: Enabled for performance (PS_HBC_PERIOD_CTL lookup)
2. **Error Handling**: ABORT() function stops session execution on date mismatch
3. **Data Types**: Proper mapping of string, decimal, and datetime types
4. **Connection Strategy**: Sequence Generator connected to ensure proper sequencing

#### Session Configuration Recommendations
1. **Commit Interval**: Set based on data volume (e.g., 10,000 rows)
2. **Error Handling**: Stop on errors due to critical date validation
3. **Performance**: Enable lookup caching and appropriate buffer sizes
4. **Logging**: Enable session logging for troubleshooting

### Deployment Steps
1. Import source and target definitions
2. Create lookup table definition for PS_HBC_PERIOD_CTL
3. Build mapping with transformations as specified
4. Configure session with appropriate connection information
5. Create workflow with session
6. Validate mapping logic and test with sample data

### Testing Strategy
1. **Unit Testing**: Test each transformation individually
2. **Date Validation**: Verify abort logic with mismatched dates
3. **NULL Handling**: Test with NULL values in all fields
4. **Sequence Generation**: Verify proper incrementing of ROW_COUNT
5. **Field Formatting**: Validate padding and case conversion logic

This implementation maintains full fidelity to the original pseudo-code while leveraging PowerCenter's native transformation capabilities for optimal performance and maintainability.
