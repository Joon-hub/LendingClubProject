# Lending Club Project

## Project Overview
As a master’s student studying Big data engineering, I developed this project to build a loan scoring system for Lending Club, a peer-to-peer lending platform. The system analyzes loan applications and borrower profiles to compute risk scores and assign loan grades. Working on this project helped me apply data engineering concepts to a practical problem, and I aimed to create a clear, functional pipeline.

## Project Structure
The project is organized as follows:

```
LendingClubProject/
├── src/                                # Source code
│   ├── loan_scorer.py                 # Core loan scoring logic
│   ├── utils/                         # Utility functions
│   │   ├── spark_utils.py             # Spark session management
│   │   ├── data_utils.py              # Data manipulation helpers
│   │   └── validation_utils.py        # Data validation functions
│   └── config/                        # Configuration files
│       ├── scoring_config.py          # Scoring parameters
│       └── paths_config.py            # File path configurations
│
├── tests/                             # Unit tests
│   ├── test_loan_scorer.py            # Loan scorer tests
│   └── test_utils/                    # Utility function tests
│       ├── test_spark_utils.py
│       ├── test_data_utils.py
│       └── test_validation_utils.py
│
├── data/                              # Data storage
│   ├── raw/                           # Raw input data
│   └── processed/                     # Processed data
│
├── loan_score/                        # Output directory
│   ├── daily/                         # Daily score files
│   ├── monthly/                       # Monthly aggregated scores
│   └── reports/                       # Score analysis reports
│
├── requirements.txt                   # Python dependencies
└── README.md                          # Project documentation
```

## ETL Pipeline Approach

### 1. Data Ingestion Layer
- **Sources**: Datasets include loan details, repayments, customer information, defaulters, and delinquency records.
- **Formats**: Input data is in CSV format; output is stored in Parquet for efficiency.
- **Quality Checks**: Validates schema, handles null values, ensures data type consistency, and detects duplicates.

### 2. Data Processing Layer
- **Cleaning**: Removes duplicates, standardizes formats, handles missing values, and validates data ranges.
- **Transformation**: Computes derived metrics, applies business rules, and generates scoring components.

### 3. Data Storage Layer
- **Strategy**: Partitions data by date, uses Parquet for optimized storage and querying.
- **Access**: Supports Spark DataFrame operations and SQL-based querying for batch processing.

### 4. Data Quality Framework
- **Validation**: Ensures completeness, accuracy, and consistency of data.
- **Monitoring**: Tracks pipeline execution, logs errors, and generates basic data quality reports.

## Implementation Guide

### 1. Environment Setup
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Data Preparation
1. Place raw data files in `data/raw/`.
2. Verify data schema using validation scripts.
3. Perform initial data quality checks.

### 3. Configuration Setup
1. Update `config/paths_config.py` with data paths.
2. Adjust `config/scoring_config.py` for scoring rules if needed.
3. Configure environment variables in a `.env` file.

### 4. Running the Pipeline
```bash
# Run full ETL pipeline
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

# Run specific tests
pytest tests/test_loan_scorer.py -v
pytest tests/test_utils/ -v
```

### 6. Monitoring and Maintenance
1. Review logs in `logs/`.
2. Check data quality reports.
3. Monitor pipeline performance and update configurations as needed.

## Loan Scoring System

### Scoring Components
The system evaluates three components:
1. **Payment History (20% weight)**: Analyzes last payment amount, total payments, and consistency.
2. **Loan Defaults (45% weight)**: Assesses delinquency, public records, bankruptcies, and credit inquiries.
3. **Financial Health (35% weight)**: Evaluates loan status, home ownership, credit utilization, and grades.

### Scoring Parameters
- Unacceptable: 0 points
- Very Bad: 100 points
- Bad: 250 points
- Good: 500 points
- Very Good: 650 points
- Excellent: 800 points

### Grade Assignment
Grades are assigned based on total score:
- A: > 2500 points
- B: 2000-2500 points
- C: 1500-2000 points
- D: 1000-1500 points
- E: 750-1000 points
- F: ≤ 750 points

## Implementation Details

### LoanScorer Class
The `LoanScorer` class contains:
1. `calculate_payment_history_score()`: Evaluates payment patterns and amounts.
2. `calculate_defaulters_history_score()`: Processes delinquency and credit records.
3. `calculate_financial_health_score()`: Analyzes financial metrics and loan status.
4. `calculate_final_loan_score()`: Combines scores, applies weights, and assigns grades.

## Output
Scores are saved in `loan_score/`:
```
loan_score/
├── daily/                             # Daily scores in Parquet
├── monthly/                           # Monthly aggregated scores
└── reports/                           # Analysis reports
```
Each file includes loan ID, customer ID, score components, final score, grade, timestamp, and metadata.

## Future Enhancements
1. Add real-time scoring capabilities.
2. Incorporate machine learning for risk prediction.
3. Develop a visualization dashboard.
4. Include additional financial health metrics.
5. Set up automated testing and monitoring.
6. Implement data lineage tracking.
7. Optimize pipeline performance.

This project was a steep learning curve, but it solidified my understanding of data engineering pipelines. Feedback is welcome as I continue to refine my skills.
