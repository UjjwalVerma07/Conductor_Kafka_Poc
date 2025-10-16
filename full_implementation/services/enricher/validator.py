#!/usr/bin/env python3
"""
Enricher Logic
Enriches validated data with additional fields and scores
"""

import json
import logging
from datetime import datetime
import random

logger = logging.getLogger(__name__)


def enrich_record(record):
    """
    Enrich a single record with additional data
    Adds quality scores, risk assessment, customer segmentation, etc.
    """
    # Extract validation results
    email_valid = record.get('email_valid', False)
    phone_valid = record.get('phone_valid', False)
    
    # Calculate data quality score (0-100)
    quality_score = 0
    if email_valid:
        quality_score += 50
    if phone_valid:
        quality_score += 50
    
    # Determine if fully verified
    verified = email_valid and phone_valid
    
    # Determine customer segment based on quality
    if quality_score >= 100:
        segment = "premium"
    elif quality_score >= 50:
        segment = "standard"
    else:
        segment = "basic"
    
    # Calculate risk score (mock: 0-100, lower is better)
    # In real implementation, this would use ML models or rule engines
    if verified:
        risk_score = random.randint(0, 30)  # Low risk
    elif quality_score >= 50:
        risk_score = random.randint(30, 60)  # Medium risk
    else:
        risk_score = random.randint(60, 100)  # High risk
    
    # Add enrichment fields to record
    enriched = dict(record)
    enriched['data_quality_score'] = quality_score
    enriched['customer_segment'] = segment
    enriched['verified'] = verified
    enriched['risk_score'] = risk_score
    enriched['enrichment_timestamp'] = datetime.utcnow().isoformat()
    enriched['enrichment_source'] = 'mock-enricher'
    
    # Optional: Add more enrichment fields
    enriched['marketing_approved'] = verified  # Only market to verified contacts
    enriched['priority_tier'] = 1 if segment == "premium" else (2 if segment == "standard" else 3)
    
    return enriched


def process_csv_data(json_content):
    """
    Process JSON data from Stage 2 (phone validator output) and enrich it
    Input: JSON string with structure {'data': [...], 'stats': {...}}
    Output: Same structure with enrichment fields added to each record
    """
    logger.info("Processing JSON data for enrichment")
    
    try:
        # Parse JSON input (output from Stage 2)
        input_data = json.loads(json_content)
        records = input_data.get('data', [])
        
        results = []
        stats = {
            'total_records': 0,
            'fully_verified': 0,      # Both email and phone valid
            'partially_verified': 0,  # Either email or phone valid
            'unverified': 0,          # Neither valid
            'premium_segment': 0,
            'standard_segment': 0,
            'basic_segment': 0,
            'low_risk': 0,            # Risk score 0-30
            'medium_risk': 0,         # Risk score 31-60
            'high_risk': 0            # Risk score 61-100
        }
        
        for record in records:
            stats['total_records'] += 1
            
            # Enrich the record
            enriched_record = enrich_record(record)
            
            # Update statistics
            email_valid = enriched_record.get('email_valid', False)
            phone_valid = enriched_record.get('phone_valid', False)
            
            # Count verification levels
            if email_valid and phone_valid:
                stats['fully_verified'] += 1
            elif email_valid or phone_valid:
                stats['partially_verified'] += 1
            else:
                stats['unverified'] += 1
            
            # Count segments
            segment = enriched_record.get('customer_segment', 'basic')
            if segment == 'premium':
                stats['premium_segment'] += 1
            elif segment == 'standard':
                stats['standard_segment'] += 1
            else:
                stats['basic_segment'] += 1
            
            # Count risk levels
            risk_score = enriched_record.get('risk_score', 100)
            if risk_score <= 30:
                stats['low_risk'] += 1
            elif risk_score <= 60:
                stats['medium_risk'] += 1
            else:
                stats['high_risk'] += 1
            
            results.append(enriched_record)
        
        logger.info(f"Enrichment complete: {stats}")
        
        return {
            'data': results,
            'stats': stats
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON input: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {e}", exc_info=True)
        raise