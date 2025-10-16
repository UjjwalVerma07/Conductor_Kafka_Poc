#!/usr/bin/env python3
"""
Email Validator Logic
Validates email addresses in CSV data
"""

from io import StringIO
import re
import csv
import json 
import logging

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

def validate_emails(email):
    if not email or not isinstance(email, str):
        return False
    email = email.strip()
    return bool(EMAIL_PATTERN.match(email))

def process_csv_data(csv_content):
    logger.info("Processing CSV data")
    #String IO allows the CSV string to be treated as a file object (so you can use it with the csv module)
    csv_file = StringIO(csv_content)
    #DictReader create a reader object that maps each row as a dictionary (key-value pairs) based on the header row
    reader = csv.DictReader(csv_file)

    results = []
    stats = {
        'total_records': 0,
        'valid_emails': 0,
        'invalid_emails': 0,
        'duplicate_emails': 0
    }
    seen_emails = set()
    for row in reader:
        stats['total_records'] += 1
        email = row.get('email', '').strip()
        is_valid = validate_emails(email)
        is_duplicate = email.lower() in seen_emails

        if is_valid:
            stats['valid_emails'] += 1
            if is_duplicate:
                stats['duplicate_emails'] += 1
            else:
                seen_emails.add(email.lower())
        else:
            stats['invalid_emails'] += 1
        #Create a new dictionary from the original row and add the additional fields
        result_row = dict(row)
        result_row['email_valid'] = is_valid
        result_row['email_duplicate'] = is_duplicate
        result_row['email_status'] = 'valid' if is_valid and not is_duplicate else ('duplicate' if is_duplicate else 'invalid')

        results.append(result_row)

    logger.info(f"Email Validation complete: {stats}")

    return {
        'data': results,
        'stats': stats
    }