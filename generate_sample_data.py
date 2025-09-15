#!/usr/bin/env python3
"""
Generate sample credit card transaction data with rich attributes for fine-grained entitlements demo.
This data includes multiple dimensions for access control: region, customer tier, department, sensitivity, etc.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import uuid
from decimal import Decimal

# Set seed for reproducible results
random.seed(42)
np.random.seed(42)

# Configuration
NUM_TRANSACTIONS = 5000
START_DATE = datetime.now() - timedelta(days=365)
END_DATE = datetime.now()

# Define entitlement dimensions
REGIONS = [
    {'code': 'US_EAST', 'name': 'US East', 'countries': ['US'], 'timezone': 'America/New_York'},
    {'code': 'US_WEST', 'name': 'US West', 'countries': ['US'], 'timezone': 'America/Los_Angeles'},
    {'code': 'EUROPE', 'name': 'Europe', 'countries': ['UK', 'DE', 'FR', 'IT', 'ES'], 'timezone': 'Europe/London'},
    {'code': 'ASIA_PAC', 'name': 'Asia Pacific', 'countries': ['JP', 'SG', 'AU', 'HK'], 'timezone': 'Asia/Tokyo'}
]

CUSTOMER_TIERS = [
    {'tier': 'PREMIUM', 'weight': 5, 'min_limit': 50000, 'max_limit': 500000},
    {'tier': 'GOLD', 'weight': 15, 'min_limit': 20000, 'max_limit': 100000},
    {'tier': 'SILVER', 'weight': 30, 'min_limit': 5000, 'max_limit': 25000},
    {'tier': 'STANDARD', 'weight': 50, 'min_limit': 1000, 'max_limit': 10000}
]

DEPARTMENTS = ['FINANCE', 'OPERATIONS', 'MARKETING', 'COMPLIANCE', 'FRAUD', 'CUSTOMER_SERVICE']

SENSITIVITY_LEVELS = [
    {'level': 'PUBLIC', 'weight': 20},
    {'level': 'INTERNAL', 'weight': 40},
    {'level': 'CONFIDENTIAL', 'weight': 30},
    {'level': 'RESTRICTED', 'weight': 10}
]

CARD_TYPES = [
    {'type': 'CREDIT', 'subtype': 'REWARDS', 'weight': 35},
    {'type': 'CREDIT', 'subtype': 'CASHBACK', 'weight': 25},
    {'type': 'CREDIT', 'subtype': 'TRAVEL', 'weight': 20},
    {'type': 'DEBIT', 'subtype': 'STANDARD', 'weight': 15},
    {'type': 'PREPAID', 'subtype': 'GIFT', 'weight': 5}
]

CARD_BRANDS = ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER']

MERCHANT_CATEGORIES = [
    {'code': '5411', 'category': 'GROCERY', 'description': 'Grocery Stores, Supermarkets'},
    {'code': '5812', 'category': 'RESTAURANT', 'description': 'Eating Places, Restaurants'},
    {'code': '5541', 'category': 'GAS', 'description': 'Service Stations'},
    {'code': '5311', 'category': 'RETAIL', 'description': 'Department Stores'},
    {'code': '5999', 'category': 'RETAIL', 'description': 'Miscellaneous Retail'},
    {'code': '4111', 'category': 'TRANSPORT', 'description': 'Transportation, Suburban and Local'},
    {'code': '5921', 'category': 'LIQUOR', 'description': 'Package Stores, Beer, Wine, Liquor'},
    {'code': '7995', 'category': 'ENTERTAINMENT', 'description': 'Betting, Gambling'},
    {'code': '6051', 'category': 'FINANCIAL', 'description': 'Non-FI, Money Orders'},
    {'code': '5691', 'category': 'APPAREL', 'description': 'Men\'s and Women\'s Clothing'}
]

TRANSACTION_STATUSES = [
    {'status': 'APPROVED', 'weight': 85},
    {'status': 'DECLINED', 'weight': 10},
    {'status': 'PENDING', 'weight': 3},
    {'status': 'CANCELLED', 'weight': 2}
]

RISK_LEVELS = [
    {'level': 'LOW', 'weight': 70},
    {'level': 'MEDIUM', 'weight': 20},
    {'level': 'HIGH', 'weight': 8},
    {'level': 'CRITICAL', 'weight': 2}
]

def weighted_choice(choices, weights):
    """Select a choice based on weights."""
    return random.choices(choices, weights=weights, k=1)[0]

def generate_transaction_id():
    """Generate a unique transaction ID."""
    return f"TXN_{uuid.uuid4().hex[:12].upper()}"

def generate_card_number(card_brand):
    """Generate a masked card number based on brand."""
    if card_brand == 'VISA':
        return f"4***-****-****-{random.randint(1000, 9999)}"
    elif card_brand == 'MASTERCARD':
        return f"5***-****-****-{random.randint(1000, 9999)}"
    elif card_brand == 'AMEX':
        return f"3***-******-{random.randint(10000, 99999)}"
    elif card_brand == 'DISCOVER':
        return f"6***-****-****-{random.randint(1000, 9999)}"
    else:
        return f"****-****-****-{random.randint(1000, 9999)}"

def generate_customer_id():
    """Generate a unique customer ID."""
    return f"CUST_{random.randint(100000, 999999)}"

def generate_merchant_id():
    """Generate a merchant ID."""
    return f"MERCH_{random.randint(10000, 99999)}"

def generate_amount(customer_tier, merchant_category):
    """Generate transaction amount based on customer tier and merchant category."""
    tier_data = next(t for t in CUSTOMER_TIERS if t['tier'] == customer_tier)
    base_max = tier_data['max_limit'] * 0.1  # Max 10% of credit limit per transaction
    
    # Adjust based on merchant category
    if merchant_category in ['GROCERY', 'GAS', 'RESTAURANT']:
        amount = random.uniform(5, min(500, base_max))
    elif merchant_category in ['RETAIL', 'APPAREL']:
        amount = random.uniform(20, min(2000, base_max))
    elif merchant_category in ['ENTERTAINMENT', 'LIQUOR']:
        amount = random.uniform(15, min(800, base_max))
    elif merchant_category == 'FINANCIAL':
        amount = random.uniform(100, min(5000, base_max))
    else:
        amount = random.uniform(10, min(1000, base_max))
    
    return round(amount, 2)

def assign_sensitivity_level(amount, risk_level, customer_tier):
    """Assign sensitivity level based on transaction characteristics."""
    if risk_level == 'CRITICAL' or customer_tier == 'PREMIUM':
        return 'RESTRICTED'
    elif risk_level == 'HIGH' or amount > 5000:
        return 'CONFIDENTIAL'
    elif risk_level == 'MEDIUM' or amount > 1000:
        return 'INTERNAL'
    else:
        return 'PUBLIC'

def generate_transaction():
    """Generate a single transaction record."""
    # Basic identifiers
    transaction_id = generate_transaction_id()
    customer_id = generate_customer_id()
    merchant_id = generate_merchant_id()
    
    # Select random attributes with weights
    region = random.choice(REGIONS)
    customer_tier_info = weighted_choice(CUSTOMER_TIERS, [t['weight'] for t in CUSTOMER_TIERS])
    card_info = weighted_choice(CARD_TYPES, [t['weight'] for t in CARD_TYPES])
    card_brand = random.choice(CARD_BRANDS)
    merchant_info = random.choice(MERCHANT_CATEGORIES)
    status_info = weighted_choice(TRANSACTION_STATUSES, [t['weight'] for t in TRANSACTION_STATUSES])
    risk_info = weighted_choice(RISK_LEVELS, [t['weight'] for t in RISK_LEVELS])
    
    # Generate dependent fields
    transaction_date = START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )
    
    amount = generate_amount(customer_tier_info['tier'], merchant_info['category'])
    sensitivity_level = assign_sensitivity_level(amount, risk_info['level'], customer_tier_info['tier'])
    
    # Assign primary department based on transaction characteristics
    if risk_info['level'] in ['HIGH', 'CRITICAL']:
        primary_department = 'FRAUD'
    elif merchant_info['category'] == 'FINANCIAL':
        primary_department = 'COMPLIANCE'
    elif amount > 10000:
        primary_department = 'FINANCE'
    elif merchant_info['category'] in ['ENTERTAINMENT', 'LIQUOR']:
        primary_department = 'COMPLIANCE'
    else:
        primary_department = random.choice(['OPERATIONS', 'CUSTOMER_SERVICE'])
    
    return {
        'transaction_id': transaction_id,
        'customer_id': customer_id,
        'merchant_id': merchant_id,
        'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
        'amount': amount,
        'currency': 'USD',
        'card_number': generate_card_number(card_brand),
        'card_brand': card_brand,
        'card_type': card_info['type'],
        'card_subtype': card_info['subtype'],
        'merchant_name': f"{merchant_info['category'].title()} Store #{random.randint(1, 999)}",
        'merchant_category_code': merchant_info['code'],
        'merchant_category': merchant_info['category'],
        'merchant_description': merchant_info['description'],
        'transaction_status': status_info['status'],
        'authorization_code': f"AUTH{random.randint(100000, 999999)}",
        'region_code': region['code'],
        'region_name': region['name'],
        'country_code': random.choice(region['countries']),
        'timezone': region['timezone'],
        'customer_tier': customer_tier_info['tier'],
        'customer_credit_limit': customer_tier_info['max_limit'],
        'primary_department': primary_department,
        'sensitivity_level': sensitivity_level,
        'risk_level': risk_info['level'],
        'risk_score': round(random.uniform(0, 100), 2),
        'is_international': random.choice([True, False]) if region['code'] != 'US_EAST' else False,
        'is_online': random.choice([True, False]),
        'processor_response_code': random.choice(['00', '05', '14', '41', '43', '51', '54', '61']),
        'created_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updated_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def main():
    """Generate the complete sample dataset."""
    print(f"Generating {NUM_TRANSACTIONS} credit card transactions...")
    
    transactions = []
    for i in range(NUM_TRANSACTIONS):
        if i % 500 == 0:
            print(f"Generated {i} transactions...")
        transactions.append(generate_transaction())
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions)
    
    # Save to CSV
    csv_file = 'credit_card_transactions.csv'
    df.to_csv(csv_file, index=False)
    print(f"Saved {len(transactions)} transactions to {csv_file}")
    
    # Generate summary statistics
    print("\n=== DATASET SUMMARY ===")
    print(f"Total transactions: {len(transactions)}")
    print(f"Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
    print(f"Amount range: ${df['amount'].min():.2f} to ${df['amount'].max():.2f}")
    print(f"Average amount: ${df['amount'].mean():.2f}")
    
    print("\n=== ENTITLEMENT DIMENSIONS ===")
    print("Regions:")
    print(df['region_code'].value_counts())
    print("\nCustomer Tiers:")
    print(df['customer_tier'].value_counts())
    print("\nSensitivity Levels:")
    print(df['sensitivity_level'].value_counts())
    print("\nPrimary Departments:")
    print(df['primary_department'].value_counts())
    print("\nRisk Levels:")
    print(df['risk_level'].value_counts())
    
    # Save metadata for reference
    metadata = {
        'dataset_info': {
            'name': 'Credit Card Transactions',
            'description': 'Sample credit card transaction data with fine-grained entitlement attributes',
            'records': len(transactions),
            'generated_date': datetime.now().isoformat(),
            'date_range': {
                'start': df['transaction_date'].min(),
                'end': df['transaction_date'].max()
            }
        },
        'entitlement_dimensions': {
            'regions': [r['code'] for r in REGIONS],
            'customer_tiers': [t['tier'] for t in CUSTOMER_TIERS],
            'departments': DEPARTMENTS,
            'sensitivity_levels': [s['level'] for s in SENSITIVITY_LEVELS],
            'risk_levels': [r['level'] for r in RISK_LEVELS]
        },
        'schema': [
            {'column': col, 'type': str(df[col].dtype), 'sample': str(df[col].iloc[0])}
            for col in df.columns
        ]
    }
    
    with open('dataset_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"\nDataset metadata saved to dataset_metadata.json")
    print("Sample data generation complete!")

if __name__ == "__main__":
    main()
