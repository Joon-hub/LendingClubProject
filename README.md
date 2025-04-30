# Lending Club Project

## Project Overview
This project implements a comprehensive loan scoring system for Lending Club, a peer-to-peer lending platform. The system analyzes various aspects of loan applications and borrower profiles to calculate risk scores and assign loan grades.

## Project Structure
```
LendingClubProject/
├── src/                                # Source code directory
│   ├── loan_scorer.py                 # Core loan scoring implementation
│   ├── utils/                         # Utility functions
│   │   ├── spark_utils.py             # Spark session management
│   │   ├── data_utils.py              # Data manipulation helpers
│   │   └── validation_utils.py        # Data validation functions
│   └── config/                        # Configuration files
│       ├── scoring_config.py          # Scoring parameters
│       └── paths_config.py            # File paths configuration
│
├── tests/                             # Test directory
│   ├── test_loan_scorer.py            # Loan scorer unit tests
│   └── test_utils/                    # Utility function tests
│       ├── test_spark_utils.py
│       ├── test_data_utils.py
│       └── test_validation_utils.py
│
├── data/                              # Data directory
│   ├── raw/                           # Raw input data
│   └── processed/                     # Processed data
│
├── loan_score/                        # Loan score output directory
│   ├── daily/                         # Daily score files
│   ├── monthly/                       # Monthly aggregated scores
│   └── reports/                       # Score analysis reports
│
├── requirements.txt                   # Python dependencies
└── README.md                          # Project documentation
```

## ETL Pipeline Approach

### 1. Data Ingestion Layer
- **Source Systems**:
  - Loan Details (`loans`)
  - Loan Repayments (`loans_repayments`)
  - Customer Information (`customers_new`)
  - Defaulters Details (`loans_defaulters_detail_rec_enq_new`)
  - Delinquency Records (`loans_defaulters_delinq_new`)
  - Bad Customer Data (`bad_customer_data_final`)

- **Data Formats**:
  - Input: CSV/Parquet files
  - Output: Parquet format for optimized storage and querying

- **Data Quality Checks**:
  - Schema validation
  - Null value handling
  - Data type consistency
  - Duplicate detection

### 2. Data Processing Layer
- **Data Cleaning**:
  - Handle missing values
  - Standardize formats
  - Remove duplicates
  - Validate data ranges

- **Data Transformation**:
  - Calculate derived metrics
  - Apply business rules
  - Create aggregations
  - Generate scoring components

### 3. Data Storage Layer
- **Storage Strategy**:
  - Partitioned by date
  - Optimized for query performance
  - Compressed for storage efficiency

- **Data Access**:
  - SQL-based querying
  - Spark DataFrame operations
  - Batch processing support

### 4. Data Quality Framework
- **Validation Rules**:
  - Completeness checks
  - Accuracy validation
  - Consistency verification
  - Timeliness monitoring

- **Monitoring**:
  - Pipeline execution tracking
  - Error logging
  - Performance metrics
  - Data quality reports

## Step-by-Step Implementation Guide

### 1. Environment Setup
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Data Preparation
1. Place raw data files in the `data/raw/` directory
2. Ensure data files follow the expected schema
3. Verify data quality using validation scripts

### 3. Configuration Setup
1. Update `config/paths_config.py` with your data paths
2. Modify `config/scoring_config.py` for custom scoring rules
3. Set up environment variables in `.env` file

### 4. Running the Pipeline
```bash
# Run the complete ETL pipeline
python src/loan_scorer.py

# Run specific components
python src/loan_scorer.py --component payment_history
python src/loan_scorer.py --component defaulters_history
python src/loan_scorer.py --component financial_health
```

### 5. Testing
```bash
# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/test_loan_scorer.py -v
pytest tests/test_utils/ -v
```

### 6. Monitoring and Maintenance
1. Check logs in `logs/` directory
2. Review data quality reports
3. Monitor pipeline performance
4. Update configurations as needed

## Loan Scoring System

### Scoring Components
The loan scoring system evaluates three main components:

1. **Payment History (20% weight)**
   - Last payment amount analysis
   - Total payment received evaluation
   - Payment consistency assessment

2. **Loan Defaults (45% weight)**
   - Delinquency history
   - Public records
   - Bankruptcy records
   - Credit inquiries

3. **Financial Health (35% weight)**
   - Loan status
   - Home ownership
   - Credit limit utilization
   - Loan grade and sub-grade

### Scoring Parameters
The system uses the following scoring parameters:
- Unacceptable: 0 points
- Very Bad: 100 points
- Bad: 250 points
- Good: 500 points
- Very Good: 650 points
- Excellent: 800 points

### Grade Assignment
Final grades are assigned based on total scores:
- A: > 2500 points
- B: 2000-2500 points
- C: 1500-2000 points
- D: 1000-1500 points
- E: 750-1000 points
- F: ≤ 750 points

## Implementation Details

### LoanScorer Class
The core functionality is implemented in the `LoanScorer` class with the following methods:

1. `calculate_payment_history_score()`
   - Evaluates payment patterns
   - Considers last payment amount relative to monthly installment
   - Assesses total payment received against funded amount

2. `calculate_defaulters_history_score()`
   - Analyzes delinquency records
   - Processes public records and bankruptcies
   - Evaluates credit inquiries

3. `calculate_financial_health_score()`
   - Assesses loan status
   - Evaluates home ownership
   - Analyzes credit limit utilization
   - Processes loan grades

4. `calculate_final_loan_score()`
   - Combines all component scores
   - Applies respective weights
   - Assigns final grades

## Output
The final loan scores and grades are saved in the following directory structure:
```
loan_score/
├── daily/                             # Daily score files in Parquet format
├── monthly/                           # Monthly aggregated scores
└── reports/                           # Score analysis reports
```

The scores are stored in Parquet format for efficient storage and querying. Each score file contains:
- Loan ID
- Customer ID
- Score components
- Final score
- Assigned grade
- Timestamp
- Processing metadata

## Future Enhancements
1. Implement real-time scoring
2. Add machine learning models for risk prediction
3. Create a dashboard for score visualization
4. Add more sophisticated financial health metrics
5. Implement automated testing pipeline
6. Add data lineage tracking
7. Implement automated data quality monitoring
8. Add performance optimization features
