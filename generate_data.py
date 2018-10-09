import random
import json
from collections import namedtuple, defaultdict
import re
import csv
from typing import Dict, Callable, Iterable

id_counts = defaultdict(lambda: 0)
def increment(id_name: str) -> int:
    """Column type for an increasing counter. Multiple counters can be used 
    simultaneously by specifying different IDs.
    
    Arguments:
        id_name {str} -- Counter ID. 
    
    Returns:
        int -- Counter value increasing by 1 each call.
    """
    id_counts[id_name] += 1
    return id_counts[id_name]


non_alpha = re.compile('[^a-zA-Z0-9]')
def make_email(name: str) -> str:
    """Makes an email address from the given name. Deletes all non 
    alpha-numeric characters and appends @example.com.
    
    Arguments:
        name {str} -- Name.
    
    Returns:
        str -- Email.
    """
    return non_alpha.sub('', name) + str(random.randint(10, 10000)) + '@example.com'


text_values_remaining = defaultdict(lambda: [])
csv_values_remaining = defaultdict(lambda: defaultdict(lambda: []))
def from_txt(filename):
    """Randomly returns full lines from a text file. Lines are not repeated
    until every line has been selected exactly once.
    
    Arguments:
        filename {str} -- File path to text file.
    
    Returns:
        str -- Random line.
    """
    if not text_values_remaining[filename]:
        _reset_txt(filename)
    return text_values_remaining[filename].pop()

def from_csv(filename: str, key: str, splice: slice=None) -> str:
    """Randomly returns values from a CSV file. Rows are not repeated until 
    every row has been used once.

    The first row should be the column headers. Splits on all commas without 
    exception. 
    
    Arguments:
        filename {str} -- File path.
        key {str} -- Column name.
    
    Keyword Arguments:
        splice {slice} -- Slice the value rows to only use a certain range. 
            (default: {None})
    
    Returns:
        str -- Random value from the given column and CSV file.
    """
    if not csv_values_remaining[filename][key]:
        _reset_csv(filename, splice)
    return csv_values_remaining[filename][key].pop()

def _reset_csv(filename, splice):
    with open(filename, newline='') as f:
        csv_values_remaining[filename].clear()

        # Parse column names.
        columns = []
        for col in next(f).rstrip().split(','):
            columns.append(col)
        
        # Shuffle before splitting so rows are kept together across all 
        # columns.
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
            l = l.rstrip() # Strip newlines.
            text_values_remaining[filename].append(l)
        random.shuffle(text_values_remaining[filename])

columns = {
    'ID': lambda x: increment('pk'),
    'Name': lambda x: from_txt('data_source/names.txt'),
    'Email': lambda x: make_email(x['Name']),
}

table = 'TICKET'

# Each dict value is called once per row to generate that column's value.
# The function is called with a dictionary of all the previous column values
# for the current row.
columns = {
    'MatchID': lambda x: from_csv('data_export/MATCH.csv', 'ID'),
    'Ticket#': lambda x: increment(x['MatchID']),
    'CustomerID': lambda x: from_csv('data_export/CUSTOMER.csv', "ID"),
    'Price': lambda x: f'{random.randint(50, 200)}.{str(random.randint(0, 99)).rjust(2, "0")}'
}
pk = ['MatchID', 'Ticket#']
n = 5000

def generate_csv(output_name, columns: Dict[str, Callable[[Dict], str]], pk, n):
    """Generates a CSV file with the given columns, primary key and number of
    rows.
    
    Arguments:
        output_name {str} -- Output CSV path.
        columns {Dict[str, Callable[[Dict], str]]} -- Column definitions (see above).
        pk {Iterable[str]} -- Primary key. The function will ensure this remains unique.
        n {int} -- Number of rows.
    """

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