import random
import json
from collections import namedtuple, defaultdict
import re
import csv

id_counts = defaultdict(lambda: 0)
def increment(id_name):
    id_counts[id_name] += 1
    return id_counts[id_name]


non_alpha = re.compile('[^a-zA-Z0-9]')
def make_email(name):
    return non_alpha.sub('', name) + str(random.randint(10, 10000)) + '@example.com'


text_values_remaining = defaultdict(lambda: [])
csv_values_remaining = defaultdict(lambda: defaultdict(lambda: []))
def from_txt(filename):
    if not text_values_remaining[filename]:
        _reset_txt(filename)
    return text_values_remaining[filename].pop()

def from_csv(filename, key, splice=None):
    if not csv_values_remaining[filename][key]:
        _reset_csv(filename, splice)
    return csv_values_remaining[filename][key].pop()

def _reset_csv(filename, splice):
    with open(filename, newline='') as f:
        csv_values_remaining[filename].clear()

        # Column names.
        columns = []
        for col in next(f).rstrip().split(','):
            columns.append(col)
        
        # We ensure rows are kept together.
        shuffled = list(f)
        if splice is not None:
            shuffled = shuffled[splice]
        random.shuffle(shuffled)
        for line in shuffled:
            line = line.rstrip().split(',')
            for i, c in enumerate(line):
                csv_values_remaining[filename][columns[i]].append(c)

def _reset_txt(filename):
    with open(filename) as f:
        text_values_remaining[filename].clear()
        for l in f:
            l = l.rstrip()
            text_values_remaining[filename].append(l)
        random.shuffle(text_values_remaining[filename])

columns = {
    'ID': lambda x: increment('pk'),
    'Name': lambda x: from_txt('data_source/names.txt'),
    'Email': lambda x: make_email(x['Name']),
}

table = 'TICKET'

columns = {
    'MatchID': lambda x: from_csv('data_export/MATCH.csv', 'ID'),
    'Ticket#': lambda x: increment(x['MatchID']),
    'CustomerID': lambda x: from_csv('data_export/CUSTOMER.csv', "ID"),
    'Price': lambda x: f'{random.randint(50, 200)}.{str(random.randint(0, 99)).rjust(2, "0")}'
}
pk = ['MatchID', 'Ticket#']
n = 5000

def generate_csv(output_name, columns: dict, pk, n):
    with open(output_name, 'w') as f:
        f.write(','.join(columns.keys()) + '\n')
        i = 0
        seen = set()
        while i < n:
            values = {}
            for col, func in columns.items():
                values[col] = str(func(values))
            this_key = tuple(values[x] for x in pk)
            if this_key in seen:
                continue
            else:
                seen.add(this_key)
                i += 1
                f.write(','.join(values.values()) + '\n')

def main():
    generate_csv(f'data_export/{table}.csv', columns, pk, n)

if __name__ == '__main__':
    main()