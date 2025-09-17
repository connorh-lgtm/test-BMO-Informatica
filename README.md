# Informatica PowerCenter ETL Implementation

This repository contains a comprehensive Informatica PowerCenter implementation for a financial balance ETL pipeline, based on pseudo-code specifications.

## Overview

This project demonstrates how to translate pseudo-code ETL logic into a complete Informatica PowerCenter mapping with all necessary transformations, business rules, and data flow components.

## Files

- **`Informatica_PowerCenter_Mapping.xml`** - Complete PowerCenter mapping XML with all transformations
- **`PowerCenter_Implementation_Guide.md`** - Detailed implementation guide with transformation specifications
- **`pseudo_ETL_code.txt`** - Original pseudo-code that served as the basis for the PowerCenter implementation

## ETL Pipeline Components

### Source
- **Flat File**: `mrp0455_inv_gl.txt`
- **Fields**: Effective_Date, Receipt_Number, Balance_Type, Amount, Account

### Target
- **Table**: `PS_HBC_INV_BAL_STG`
- **Columns**: FI_INSTRUMENT_ID, FI_IBALTYPE_CD, ACCOUNT, SEQUENCENO, PF_TRANS_DT, HBC_RECEIPT_NO, FI_BALANCE_AMT

### Key Transformations

1. **Filter Transformation**: Removes records with NULL receipt numbers
2. **Lookup Transformation**: Validates dates against `PS_HBC_PERIOD_CTL` table
3. **Expression Transformations**: 
   - Date validation with abort logic
   - Field construction and formatting
   - NULL handling and data type conversions
4. **Sequence Generator**: Global ROW_COUNT for record sequencing

### Critical Business Logic

- **Date Validation**: Session aborts if Effective_Date ≠ HBC_DATA_DATE
- **Field Padding**: FI_INSTRUMENT_ID = "INV" + left-padded Receipt_Number to 42 digits
- **NULL Handling**: Consistent space defaults and proper null checks
- **Decimal Conversion**: FI_BALANCE_AMT with 3 decimal places

## Implementation Features

- Complete PowerCenter XML mapping definition
- All transformation logic preserved from pseudo-code
- Proper field mappings and data type handling
- Error handling with abort conditions
- Sequence generation for record tracking

## Usage

1. Import the XML mapping into your Informatica PowerCenter environment
2. Configure source and target connections
3. Review the implementation guide for detailed transformation specifications
4. Test with sample data to validate the ETL logic

## Architecture

The mapping follows a standard ETL pattern:
```
Source → Filter → Expression → Lookup → Expression → Target
                                ↑
                        Sequence Generator
```

This implementation serves as a proof of concept for translating pseudo-code ETL specifications into production-ready Informatica PowerCenter mappings.
