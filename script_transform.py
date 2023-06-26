import os
import re
import subprocess
import requests
from requests_negotiate_sspi import HttpNegotiateAuth

mirror_url = r'C:\Users\sebas\Documents\GitHub\Blankfactor\ethos-snowflake-mirror'
delta_dir = 'SDM\RDS\Delta'
history_dir = 'SDM\RDS\History'
script_name = 'PARTICIPANT_ACCOUNT_ROLLOVER_DISBURSEMENT_delta.sql'

delta_path = delta_dir + script_name + '_delta.sql'
history_path = history_dir + script_name + '_history.sql'

with open(os.path.join(mirror_url, delta_dir, script_name), 'r') as file:
  script = file.read()

# Returns the minimum index of a list different than -1, if there is
def get_min_index(indexes):
    filtered_numbers = [num for num in indexes if num != -1]
    if filtered_numbers:
        return min(filtered_numbers)
    else:
        return -1

# Returns the start position of a given term in the content
def find(content, str, start):
  return content.upper().find(str.upper(), start)

# Returns the end position of a given term in the content
def find_end(content, str, start):
  return content.upper().find(str.upper(), start) + len(str)

# Getting the query
insert_index = find(script, 'INSERT INTO IDENTIFIER', 0)
query_start = get_min_index([find(script, 'WITH', insert_index), find(script, 'SELECT', insert_index)])
query_end = find(script, ';', find(script, 'WHERE', query_start))
query = script[query_start:query_end]

"""
Gets the table name
Variables can have one of these formats (* means any string):
NAME * DEFAULT * <'VALUE'><OTHER_VARIABLE>;
NAME := * <'VALUE'><OTHER_VARIABLE>;
"""
def get_table_name(name):
  index = find(script, name, 0)
  start = get_min_index([find_end(script, ':=', index), find_end(script, 'DEFAULT', index)])
  end = find(script, ';', start)
  words = script[start:end].strip().split()
  value = words[-1]
  if any(word == 'RAW' and word == 'HISTORY' for word in words):
    context = '{{source_hist_context}}'
  elif any(word == 'RAW' for word in words):
    context = '{{source_context}}'
  elif any(word == 'CUR' for word in words):
    context = '{{target_context}}'
  [word for word in words if 'CUR' in word]
  if("'" in value):
    table_name = value.replace("'", '').split('.')[-1]
  else:
    table_name = get_table_name(value)
  return table_name

def replace_tables():
  identifier_end = find_end(query, 'IDENTIFIER(:', 0)
  if(identifier_end > -1):
    variable_name = query[identifier_end:find(query, ')', identifier_end)]
    get_table_name(variable_name)
  else:
    return

print(get_table_name(variable_name))

# Perform modifications on the script content
# modified_content = script_content.replace('<<variable_name>>', 'variable_value')
# modified_content = re.sub(r'{%.*?%}', '', modified_content)  # Remove Jinja tags

# Write the modified script to the automation repository
# with open(os.path.join(automation_dir, filename), 'w') as file:
#     file.write(modified_content)