import csv
import io
import os
import sys
import time

from pprint import pprint

from sortedcontainers.sortedlist import SortedList, SortedListWithKey

# TODO: handle birth age as an integer
# TODO: handle threshold percent setting
# TODO: output discerned validation settings to an INI file
# TODO: import discerned validation settings into program
# TODO: import various validation tables

vld_file_name = '/home/data/voters/nc/ncvoter48.csv'
dta_file_name = '/home/data/voters/nc/ncvoter_Statewide.csv'
dta_file_name = '/home/data/voters/nc/ncvoter92.csv'

flush_count = 10000
max_vld_rows = 0
max_dta_rows = 0

vld_values = {}
min_max_sizes = {}
min_max_values = {}
dta_value_types = {}
dta_value_counts = {}
vld_values_missing = {}
is_unique = {}

fieldnames = []

with io.open(vld_file_name, 'r', newline='') as vld_file:
    csv_reader = csv.DictReader(vld_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"')
    fieldnames = csv_reader.fieldnames
    print('vld_values_columns: %d' % len(fieldnames))
    rows = 0
    bgn_time = time.clock()
    for row in csv_reader:
        rows += 1
        for hdr_col_name in csv_reader.fieldnames:
            if hdr_col_name not in vld_values:
                vld_values[hdr_col_name] = {}
            dta_col_value = row[hdr_col_name]
            if dta_col_value not in vld_values[hdr_col_name]:
                vld_values[hdr_col_name][dta_col_value] = 1
            else:
                vld_values[hdr_col_name][dta_col_value] += 1
        if flush_count > 0 and rows % flush_count == 0:
            elapsed_secs = time.clock() - bgn_time
            rows_per_sec = rows / elapsed_secs
            print('Rows: %d, seconds: %f, rows_per_sec: %d' % (rows, elapsed_secs, rows_per_sec))
        if max_vld_rows > 0 and rows >= max_vld_rows:
            break

with io.open(dta_file_name, 'r', newline='') as dta_file:
    csv_reader = csv.DictReader(dta_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"')
    rows = 0
    bgn_time = time.clock()
    for row in csv_reader:
        rows += 1
        for hdr_col_name in csv_reader.fieldnames:
            dta_col_value = row[hdr_col_name].strip()
            len_dta_col_value = len(dta_col_value)
            if dta_col_value not in vld_values[hdr_col_name]:
                if hdr_col_name not in vld_values_missing:
                    vld_values_missing[hdr_col_name] = {}
                if dta_col_value not in vld_values_missing[hdr_col_name]:
                    vld_values_missing[hdr_col_name][dta_col_value] = 1
                else:
                    vld_values_missing[hdr_col_name][dta_col_value] += 1
            if hdr_col_name not in min_max_values:
                dta_value_types[hdr_col_name] = {'is_alnum': 0, 'is_alpha':0, 'is_digit':0, 'is_lower':0, 'is_null':0, 'is_none':0, 'is_numeric':0, 'is_space':0, 'is_upper':0, 'is_other': 0}
                min_max_sizes[hdr_col_name] = (len_dta_col_value, len_dta_col_value)
                min_max_values[hdr_col_name] = (dta_col_value, dta_col_value)
                dta_value_counts[hdr_col_name] = {}
                is_unique[hdr_col_name] = True
            if dta_col_value not in dta_value_counts[hdr_col_name]:
                dta_value_counts[hdr_col_name][dta_col_value] = 1
            else:
                dta_value_counts[hdr_col_name][dta_col_value] += 1
                is_unique[hdr_col_name] = False
            if len_dta_col_value < min_max_sizes[hdr_col_name][0]:
                min_max_sizes[hdr_col_name] = (len_dta_col_value, min_max_sizes[hdr_col_name][1])
            if len_dta_col_value > min_max_sizes[hdr_col_name][1]:
                min_max_sizes[hdr_col_name] = (min_max_sizes[hdr_col_name][0], len_dta_col_value)
            if dta_col_value < min_max_values[hdr_col_name][0]:
                min_max_values[hdr_col_name] = (dta_col_value, min_max_values[hdr_col_name][1])
            if dta_col_value > min_max_values[hdr_col_name][1]:
                min_max_values[hdr_col_name] = (min_max_values[hdr_col_name][0], dta_col_value)
            is_other = True
            if dta_col_value.isalnum():
                dta_value_types[hdr_col_name]['is_alnum'] += 1
                is_other = False
            if dta_col_value.isalpha():
                dta_value_types[hdr_col_name]['is_alpha'] += 1
                is_other = False
            if dta_col_value.isdigit():
                dta_value_types[hdr_col_name]['is_digit'] += 1
                is_other = False
            if dta_col_value.islower():
                dta_value_types[hdr_col_name]['is_lower'] += 1
                is_other = False
            if dta_col_value is None:
                dta_value_types[hdr_col_name]['is_none'] += 1
                is_other = False
            if dta_col_value == '':
                dta_value_types[hdr_col_name]['is_null'] += 1
                is_other = False
            if dta_col_value.isnumeric():
                dta_value_types[hdr_col_name]['is_numeric'] += 1
                is_other = False
            if dta_col_value.isspace():
                dta_value_types[hdr_col_name]['is_space'] += 1
                is_other = False
            if dta_col_value.isupper():
                dta_value_types[hdr_col_name]['is_upper'] += 1
                is_other = False
            if is_other:
                dta_value_types[hdr_col_name]['is_other'] += 1

        if flush_count > 0 and rows % flush_count == 0:
            elapsed_secs = time.clock() - bgn_time
            rows_per_sec = rows / elapsed_secs
            print('Rows: %d, seconds: %f, rows_per_sec: %d' % (rows, elapsed_secs, rows_per_sec))
        if max_dta_rows > 0 and rows >= max_dta_rows:
            break

elapsed_secs = time.clock() - bgn_time
rows_per_sec = rows / elapsed_secs
print('Rows: %d, seconds: %f, rows_per_sec: %d' % (rows, elapsed_secs, rows_per_sec))

elapsed_secs = time.clock() - bgn_time
rows_per_sec = rows / elapsed_secs
print('Rows: %d, seconds: %f, rows_per_sec: %d' % (rows, elapsed_secs, rows_per_sec))

pprint(is_unique)
pprint(dta_value_types)
pprint(min_max_sizes)

