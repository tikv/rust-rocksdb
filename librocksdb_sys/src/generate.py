#!/usr/bin/env python3

import re
import sys

root = '../rocksdb/include/rocksdb'

tasks = [
    (
        'DBStatisticsTickerType',
        [
            (
                '../rocksdb/include/rocksdb/statistics.h',
                re.compile(r'enum Tickers .* {'),
                re.compile(r'\s*\w(_\w)*.*'),
                re.compile(r'};\s*'),
            ),
            (
                '../libtitan_sys/titan/include/titan/statistics.h',
                re.compile(r'enum TickerType .* {'),
                re.compile(r'\s*\w(_\w)*.*'),
                re.compile(r'};\s*'),
            ),
        ],
    ),
    (
        'DBStatisticsHistogramType',
        [
            (
                '../rocksdb/include/rocksdb/statistics.h',
                re.compile(r'enum Histograms .* {'),
                re.compile(r'\s*\w(_\w)*.*'),
                re.compile(r'};\s*'),
            ),
            (
                '../libtitan_sys/titan/include/titan/statistics.h',
                re.compile(r'enum HistogramType .* {'),
                re.compile(r'\s*\w(_\w)*.*'),
                re.compile(r'};\s*'),
            ),
        ],
    ),
]

print('/// This file is generated from generate.py.')
print('/// Re-generate it if you upgrade to a new version of RocksDB.')
print('')

for task in tasks:
    rust_name = task[0]
    subtasks = task[1]
    count = 0
    variables = {}
    print('#[derive(Copy, Clone, Debug, Eq, PartialEq)]')
    print('#[repr(u32)]')
    print('pub enum {} {{'.format(rust_name))
    for subtask in subtasks:
        begin = False
        file_path = subtask[0]
        cpp_start_pattern = subtask[1]
        cpp_entry_pattern = subtask[2]
        cpp_end_pattern = subtask[3]
        for line in open(file_path):
            if not begin:
                if cpp_start_pattern.match(line):
                    begin = True
                continue
            if cpp_end_pattern.match(line):
                break
            if not cpp_entry_pattern.match(line):
                continue
            # remove return line
            line = line.strip()
            tokens = line.split(',')[0].split('=')
            if len(tokens) == 1:
                name = tokens[0].strip(' ')
                value = count
            elif len(tokens) == 2:
                name = tokens[0].strip(' ')
                value = tokens[1].strip(' ')
                if value in variables:
                    value = variables[value]
                count = int(value)
            else:
                sys.exit("invalid enum: " + line)
            if name.endswith("ENUM_MAX"):
                variables[name] = value
                continue
            name = ''.join([w.capitalize() for w in name.split('_')])
            count = count + 1
            print('    {} = {},'.format(name, value))
    print('}')
