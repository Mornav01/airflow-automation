# import os
# import django
# import sys

# # Set up Django environment
# sys.path.append(
#     "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/DeIdentification/deIdentification"
# )
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
# os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
# django.setup()

# from core.process.rules.unstruct.generic_pattern import GenericPatternDeIdentification


# text = "rohit 14-8-1999, 14/08/1999, 14-08-1999, 14 Aug 1999"
# text = "https://abc.com, 192.168.0.1, 8.8.8.8, 123.45.67.89 14-8-1999, 14/08/1999, 14-08-1999, 14 Aug 1999"
# text = "https://abc.com, 192.168.0.1, 1999/08/14, 14/08/1999, 1999/14/08, 14-08-1999"
# offset_value = 10
# date_parse_cache = {}
# deidentifier = GenericPatternDeIdentification(text, offset_value, date_parse_cache)
# output = deidentifier.deidentify()
# print(output)

dates = """
1999/08/14, 14/08/1999, 1999/14/08, 14/08/1999
1999-08-14, 14-08-1999, 1999-14-08, 14-08-1999
14-8-1999, 14/08/1999, 14-08-1999, 14 Aug 1999
Aug 1999, October 1999, 1999 October,
Aug15 1999, 15 aug 1999, 1999 aug 15
"""

# from datetime import datetime
# import re

# from datetime import datetime
# import re

# from datetime import datetime, date
# import re

# def offset_date(input_date_str: str, offset_value: int) -> tuple:
#     """
#     Parse various date formats and return offset date and year if present.
    
#     Args:
#         input_date_str (str): Date string in various formats
#         offset_value (int): Number of days to offset the date
    
#     Returns:
#         tuple: (offsetted_date_str, year or None)
#         offsetted_date_str will be in format 'DD-MM-YYYY' or 'DD MMM YYYY'
#         depending on the input format
#     """
#     # Clean the input string
#     input_date_str = input_date_str.strip()
    
#     # First try common numeric formats
#     numeric_formats =  [
#         '%Y/%m/%d', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y',
#         '%d-%m-%Y', '%d/%m/%Y', '%d %b %Y', '%b %Y',
#         '%B %Y', '%Y %B', '%b%d %Y', '%d %b %Y',
#         '%Y %b %d', '%d-%m-%y', '%Y/%d/%m', '%Y-%d-%m'
#     ]
    
#     # Try numeric formats first
#     for fmt in numeric_formats:
#         try:
#             parsed_date = datetime.strptime(input_date_str, fmt)
#             has_year = True
            
#             # Apply offset
#             offsetted_date = parsed_date.toordinal() + offset_value
#             result_date = datetime.fromordinal(offsetted_date)
            
#             # Keep the same format style (/ or -) as input
#             separator = '/' if '/' in input_date_str else '-'
#             result_str = result_date.strftime(f'%d{separator}%m{separator}%Y')
            
#             return (result_str, result_date.year)
#         except ValueError:
#             continue
    
#     # If numeric formats fail, try text-based month formats
#     # Add space between numbers and letters if not present
#     input_date_str = re.sub(r'(\d+)([A-Za-z]+)', r'\1 \2', input_date_str)
#     input_date_str = re.sub(r'([A-Za-z]+)(\d+)', r'\1 \2', input_date_str)
    
#     # Extract year if present in the input
#     year_pattern = r'\b\d{4}\b'  # Match only 4-digit years
#     year_match = re.search(year_pattern, input_date_str)
#     has_year = bool(year_match)
    
#     # Try to parse the components
#     month_pattern = r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december)'
#     day_pattern = r'\b\d{1,2}\b'
    
#     month_match = re.search(month_pattern, input_date_str.lower())
#     if not month_match:
#         raise ValueError(f"Unable to parse date: {input_date_str}")
    
#     day_matches = re.findall(day_pattern, input_date_str)
    
#     # Get month
#     month_str = month_match.group(1)[:3].title()
    
#     # Get day
#     day = 1  # default
#     for d in day_matches:
#         d = int(d)
#         if d <= 31:
#             day = d
#             break
    
#     # Get year if present
#     year = None
#     if has_year:
#         year = int(year_match.group())
    
#     # Create a base date for calculations
#     current_year = date.today().year if has_year else 2000  # Use 2000 as base year if no year provided
    
#     # Parse the date
#     try:
#         base_date = datetime.strptime(f"{day} {month_str} {current_year}", '%d %b %Y')
        
#         # Apply offset
#         offsetted_date = base_date.toordinal() + offset_value
#         result_date = datetime.fromordinal(offsetted_date)
        
#         # Format the result based on whether year was in input
#         if has_year:
#             result_str = result_date.strftime('%d %b %Y')
#         else:
#             result_str = result_date.strftime('%d %b')
            
#         return (result_str, year)
        
#     except ValueError as e:
#         raise ValueError(f"Unable to parse date: {input_date_str}")

# out = offset_date("15/08/1999", 15)
# out = offset_date("1999/15/08", 15)
# out = offset_date("15Aug 1999", 15)
# out = offset_date("15 Oct", 25)
# print(out)