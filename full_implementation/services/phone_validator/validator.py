#!/usr/bin/env python3
"""
Phone Validator Logic
Validates phone numbers in JSON data
"""

import json
import logging
from site import addsitedir
import phonenumbers
from phonenumbers import NumberParseException

logger = logging.getLogger(__name__)

def validate_phone(phone):
    if not phone or not isinstance(phone,str):
        return False
    
    phone=phone.strip()
    if not phone:
        return False
    try:
        parsed = phonenumbers.parse(phone,"IN")

        is_valid=phonenumbers.is_valid_number(parsed)
        return is_valid
    except NumberParseException as e:
        return False
    except Exception as e:
        logger.debug(f"Error validating phone number: {phone}: {e}")
        return False

def process_csv_data(json_content):
    """
    Process JSON data from Stage1(email_validator output) and validates phone numbers
    Input: JSON string with structure {'data': [...], 'stats': {...}}
    Output Same structure with phone validation fields added 
    """

    logger.info("Processing JSON data for phone validation")
    try:
        #Parsse JSON input (output from Stage 1)
        input_data=json.loads(json_content)
        records=input_data.get('data',[])
        
        results=[]
        stats={
            'total_records':0,
            'valid_phones':0,
            'invalid_phones':0,
            'duplicate_phones':0
        }
        seen_phones=set()
        for record in records:
            stats['total_records']+=1
            phone=record.get('phone','').strip()
            is_valid=validate_phone(phone)

            normalized_phone=phone.lower().replace('-','').replace(' ','').replace('(','').replace(')','')
            is_duplicate=normalized_phone in seen_phones and normalized_phone!=''
            if is_valid:
                stats['valid_phones']+=1
                if is_duplicate:
                    stats['duplicate_phones']+=1
                else:
                    seen_phones.add(normalized_phone)
            else:
                stats['invalid_phones']+=1
            

            result_row=dict(record)
            result_row['phone_valid']=is_valid
            result_row['phone_duplicate']=is_duplicate
            result_row['phone_status']='valid' if is_valid and not is_duplicate else ('duplicate' if is_duplicate else 'invalid')

            results.append(result_row)
        
        logger.info(f"Phone Validation complete: {stats}")

        return {
            'data':results,
            'stats':stats
        }
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing phene validation: {e}")
        raise
    