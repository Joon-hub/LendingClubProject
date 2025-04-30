"""
Scoring configuration module.
This module contains all the scoring parameters and thresholds used in the loan scoring system.
"""

# Payment History Scoring Points
PAYMENT_HISTORY_POINTS = {
    'excellent': 100,
    'very_good': 80,
    'good': 60,
    'bad': 40,
    'very_bad': 20
}

# Default History Scoring Points
DEFAULT_HISTORY_POINTS = {
    'excellent': 100,
    'very_good': 80,
    'good': 60,
    'bad': 40,
    'very_bad': 20
}

# Financial Health Scoring Points
FINANCIAL_HEALTH_POINTS = {
    'excellent': 100,
    'very_good': 80,
    'good': 60,
    'bad': 40,
    'very_bad': 20
}

# Grade Thresholds
GRADE_THRESHOLDS = {
    'A': 90,
    'B': 80,
    'C': 70,
    'D': 60,
    'E': 50
}

# Component Weights
COMPONENT_WEIGHTS = {
    'payment_history': 0.20,
    'defaulters_history': 0.45,
    'financial_health': 0.35
}

# Payment Thresholds
PAYMENT_THRESHOLDS = {
    'excellent': 100,
    'very_good': 80,
    'good': 60,
    'bad': 40,
    'very_bad': 20
}

# Defaulters history thresholds
DEFAULTERS_THRESHOLDS = {
    'delinq_excellent': 0,      # No delinquencies
    'delinq_good': 1,           # 1 delinquency
    'delinq_fair': 2,           # 2 delinquencies
    'delinq_poor': 3,           # 3 delinquencies
    'delinq_very_poor': 4,      # 4 or more delinquencies
    
    'pub_rec_excellent': 0,     # No public records
    'pub_rec_good': 1,          # 1 public record
    'pub_rec_fair': 2,          # 2 public records
    'pub_rec_poor': 3,          # 3 public records
    'pub_rec_very_poor': 4,     # 4 or more public records
    
    'inq_excellent': 0,         # No inquiries
    'inq_good': 1,              # 1 inquiry
    'inq_fair': 2,              # 2 inquiries
    'inq_poor': 3,              # 3 inquiries
    'inq_very_poor': 4          # 4 or more inquiries
}

# Financial health thresholds
FINANCIAL_HEALTH_THRESHOLDS = {
    'credit_limit_excellent': 0.2,    # 20% or less utilization
    'credit_limit_good': 0.4,         # 40% or less
    'credit_limit_fair': 0.6,         # 60% or less
    'credit_limit_poor': 0.8,         # 80% or less
    'credit_limit_very_poor': 1.0     # More than 80%
} 