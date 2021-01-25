from bs4 import BeautifulSoup
from lxml import html
import requests
import pandas as pd 
import re
from collections import OrderedDict

def select_regions(regions:list, html_tree, column:int):
    """Find a list of councils per specified region"""
    
    base_url = 'https://www.cvk.gov.ua/pls/vm2020/'
    return [
        (
            region + ' область',
            base_url + html_tree.xpath(f"//td[text()[contains(.,'{region}')]]/../td[{column}]/a/@href")[0]
        )
        for region in regions
    ]


def select_councils(url_list:list):
    """Form a nested dictionary to map region and council to respective url"""
    
    base_url = 'https://www.cvk.gov.ua/pls/vm2020/'
    url_dict = OrderedDict()
    for (region, url) in url_list:
        url_dict[region] = OrderedDict()
        # Request the page 
        radas = requests.get(url) 
        # Parsing the page 
        radas_tree = BeautifulSoup(radas.content, "lxml")
        # Accumulate all tables per region
        for k, i in enumerate(radas_tree.find_all('table')[2].tbody.find_all('tr')):
            a = i.find('td').find('a')
            if a:
                rada = a.text.split(',')[-1].strip()
                inner_url = base_url + a.attrs['href']
                url_dict[region][rada] = inner_url
    return url_dict


def concat_all_councils(url_dict):
    """Concatenation of all council tables"""
    
    for i, (region, councils) in enumerate(url_dict.items()):
        for k, (council, url) in enumerate(councils.items()):
            if (i == 0) and (k == 0):
                table = prepare_table(region, council, url)
            else:
                table = table.append(prepare_table(region, council, url), ignore_index=True)
    return table


def get_data(url, regions, column:int):
    """Perform preprocessing steps""" 
    
    page =  requests.get(url)
    tree = html.fromstring(page.content)
    regions = select_regions(regions_of_interest, tree, column)
    councils = select_councils(regions)
    table = concat_all_councils(councils)
    return table


def find_dates(dt):
    """Retrieve date pattern"""
    
    date_pattern = r'[0-9]{2}\.[0-9]{2}\.[0-9]{4}'
    return re.findall(date_pattern, dt)[0]


def repl(m):
    """Adds a space between found patterns in the string"""
    
    return (m.group(0)[:1] + ' ' + m.group(0)[1:])


def get_party(row, intervals:dict):
    """Collect party label per candidate from index column"""
    
    idx = int(row['index'])
    for i in intervals.keys():
        if idx < i:
            return intervals[i]
    return next(reversed(intervals.values()))
        
    
def prepare_table(region, rada, url):
    """Preprocess single table with candidates info"""
    
    input_table = pd.read_html(url, header=0, match='Відомості', flavor='lxml')[0]
    # Separate rows with party names from candidates list
    try:
        parties = pd.DataFrame(
            input_table.loc[input_table.iloc[:, 0].str.lower().str.contains('партія') == True]
        )
        candidates = pd.DataFrame(
            input_table.loc[input_table.iloc[:, 0].str.lower().str.contains('партія') == False]
        )
        # Create a dictionary to map index intervals to the party label
        inter_val = list(parties.index[1:])
        inter_val.append(len(input_table) - 1)
        intervals = OrderedDict({idx: party for (idx, party) in zip(inter_val, list(parties.iloc[:, 0].values))})
        # Create index column  
        candidates['index'] = candidates.index
        # Create a new column with respective party label
        candidates['Партія'] = candidates.apply(get_party, intervals=intervals ,axis=1)
        # Remove index column
        candidates.drop('index', axis=1, inplace=True)
    except: 
        # No rows indicating party names found, skipping transform step
        candidates = input_table
    candidates['Регіон'] = region
    candidates['Рада'] = rada
    return fix_name(candidates)


def fix_name(table):
    """Clean data to further join on ID"""
    
    # Fix name column tokenization
    col = [col for col in table.columns if 'Прізвище' in col]
    if col:
        table.insert(
            1,
            'ПІБ',
            table[col[0]].str.replace(
                r"[а-щьюяґєії][А-ЩЬЮЯҐЄІЇ]", 
                repl, 
                regex=True)
        )
        table.drop(col, axis=1, inplace=True)
        
    # Separate date of birth 
    col = [col for col in table.columns if 'Дата' in col]
    if col:
        table.insert(1, 'Дата народження', table[col[0]].apply(lambda dt : find_dates(dt)))
        table.insert(1, 'Місце народження', table[col[0]].str.replace('\d+', '', regex=True).str.strip('.'))
        table.drop(col, axis=1, inplace=True)
    else:
        # If not found try to collect date from the general info
        col = [col for col in table.columns if 'Відомості' in col]
        if col:
            table.insert(1, 'Дата народження', table[col[0]].apply(lambda dt : find_dates(dt)))
    return table


def join_tables(table_1, table_2, clean=False): 
    """Merge candidates and winners table and create status based on intersection"""
    
    keys = ['ПІБ', 'Дата народження', 'Партія', '№ ТВО, за яким закріплено', 'Регіон', 'Рада']
    join = pd.merge(table_1, table_2, how='outer', on=keys, indicator='статус', validate="m:1")
    # Create status column for canditates
    join['статус'] = join['статус'].str.replace("both", "обрано", regex=False)
    join['статус'] = join['статус'].str.replace("left_only", "не обрано", regex=False)
    # Winners which are not in the candidate list
    join['статус'] = join['статус'].str.replace("right_only", "обрано", regex=False)
    return join


def counter(j): 
    """Group and calculate counts"""
    
    all_count = j.groupby(['Партія', 'Регіон', 'Рада']).size()
    lose_count = j[j['статус'] == 'не обрано'].groupby(['Партія', 'Регіон', 'Рада']).size()
    stat = all_count.to_frame()
    stat.insert(1, 1, all_count.sub(lose_count, fill_value=0).astype(int))
    stat.rename(columns={0:'Кандидатів', 1:'Обрано'}, inplace=True)
    stat.reset_index()
    return stat


if __name__ == '__main__':
    
    regions_of_interest = [
        'Вінницька',
        'Волинська', 
        'Дніпропетровська'
    ]
    print('Fetching candidates data...')
    candidates = get_data(
        url='https://www.cvk.gov.ua/pls/vm2020/pvm008pt001f01=695pt00_t001f01=695.html',
        regions=regions_of_interest,
        column=4
    )
    print('Fetching elected candidates data...')
    winners = get_data(
        url='https://www.cvk.gov.ua/pls/vm2020/pvm002pt001f01=695pt00_t001f01=695.html',
        regions=regions_of_interest,
        column=7
    )

    candidates.to_csv('candidates.csv', index=False)
    winners.to_csv('winners.csv', index=False)
    print('Joining tables')
    both = join_tables(candidates, winners)
    both.to_csv('merged.csv', index=False)
    print('Calculating statistics   ')
    counter(both).sort_values('Обрано', ascending=False).to_csv('stats.csv')
    print('Completed.')