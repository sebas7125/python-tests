import os
import re
import subprocess
import requests
from requests_negotiate_sspi import HttpNegotiateAuth

mirror_url = r'C:\Users\sebas\Documents\GitHub\Blankfactor\ethos-snowflake-mirror'
delta_dir = 'SDM\RDS\Delta'
history_dir = 'SDM\RDS\History'
script_name = 'PARTICIPANT_ACCOUNT_ROLLOVER_DISBURSEMENT'

delta_path = os.path.join(mirror_url, delta_dir, script_name + '_delta.sql')
history_path = os.path.join(mirror_url, history_dir, script_name + '_hist.sql')


# Return the minimum index of a list different than -1, if there is
def get_min_index(indexes):
    filtered_numbers = [num for num in indexes if num != -1]
    if filtered_numbers:
        return min(filtered_numbers)
    else:
        return -1

# Return the start position of a given term in the content
def find(content, str, start):
  return content.upper().find(str.upper(), start)

# Return the end position of a given term in the content
def find_end(content, str, start):
  result = content.upper().find(str.upper(), start)
  return -1 if result == -1 else result + len(str)

"""
Get the context and table name
Variables can have one of these formats (* means any string):
NAME * DEFAULT * <'VALUE'><OTHER_VARIABLE>;
NAME := * <'VALUE'><OTHER_VARIABLE>;
"""
def get_table_definition(script, name):
  index = find(script, name, 0)
  start = get_min_index([find_end(script, ':=', index), find_end(script, 'DEFAULT', index)])
  end = find(script, ';', start)
  table = {'context': '', 'name': ''}
  words = script[start:end].strip().upper().split()
  value = words[-1]
  context = ''
  if any(word == 'RAW_DB' for word in words):
    if any('HISTORY' in word for word in words):
      context = '{{source_hist_context}}'
    else:
      context = '{{source_context}}'
  elif any(word == 'CUR_DB' for word in words):
    context = '{{target_context}}'
  table['context'] = context
  # Return context and table name in a dictionary. If there's no context in the line of the variable set, just returns table name.
  if("'" in value):
    if context:
      table['name'] = value.replace("'", '').split('.')[-1]
    else:
      table = value.replace("'", '').split('.')[-1]
  else:
    table['name'] = get_table_definition(script, value)
  return table

# Replace IDENTIFIER(:VARIABLE_NAME) for the table name and its context
def replace_tables(script):
  global query
  identifier_end = find_end(query, 'IDENTIFIER(:', 0)
  if(identifier_end > -1):
    variable_name = query[identifier_end:find(query, ')', identifier_end)]
    table = get_table_definition(script, variable_name)
    query = query.replace(f'IDENTIFIER(:{variable_name})', table['context'] + '.' + table['name'])
    replace_tables(script)
  else:
    return

# Add Jinja tags: delta and history, for UNION ALL clauses of source_context and source_hist_context

# Reading the files
with open(delta_path, 'r') as file:
  delta_script = file.read()
with open(history_path, 'r') as file:
  history_script = file.read()

# Getting the query
insert_index = find(delta_script, 'INSERT INTO IDENTIFIER', 0)
query_start = get_min_index([find(delta_script, 'WITH', insert_index), find(delta_script, 'SELECT', insert_index)])
query_end = find(delta_script, ';', find(delta_script, 'WHERE', query_start))
query = delta_script[query_start:query_end]

# Replacing table identifiers for the name and context of the tables
replace_tables(delta_script)


print(query)

# Perform modifications on the script content
# modified_content = script_content.replace('<<variable_name>>', 'variable_value')
# modified_content = re.sub(r'{%.*?%}', '', modified_content)  # Remove Jinja tags

# Write the modified script to the automation repository
# with open(os.path.join(automation_dir, filename), 'w') as file:
#     file.write(modified_content)