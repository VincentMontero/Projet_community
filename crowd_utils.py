from pymongo import MongoClient
from sys import exit
import pandas as pd
import sys
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import re
from scripts_google_sheet import write_google_sheet, read_google_sheet, get_title_sheet, get_list_sheet_name
#driver.current_url
import os
from pyairtable import Api

"""SCRAPPNIG UTILS"""
NOPRINT_TRANS_TABLE = {
    i: None for i in range(0, sys.maxunicode + 1) if not chr(i).isprintable()
}

def make_printable(s):
    return s.translate(NOPRINT_TRANS_TABLE)

""" Get wanted data from the dictionnary and insert it in the dataframe """
def stock_company_data(df, dico, COLUMNS):
    tmp = [dico[key] for key in COLUMNS]

    df.loc[len(df.index)] = tmp
    print(f"LISTE = {tmp}")
    return df

def substring_after(s, delim):
    return make_printable(s.partition(delim)[2])

def my_strstr(s1, s2):
    return make_printable(s1[s1.index(s2) + len(s2):])

def concatenate_numbers_from_index(numbers, start_index):
    concatenated_number = ""
    
    if start_index < 0 or start_index >= len(numbers):
        return None
    
    for i in range(start_index, len(numbers)):
        concatenated_number += str(numbers[i])
    
    result = int(concatenated_number)
    return result

def find_links_in_webpage(soup, name):
    try:
        links = soup.find_all('a')
        
        all_links = [link.get('href') for link in links if link.get('href') and name in link.get('href')]
        
        return all_links
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def remove_substring(original_string, substring_to_remove):
    if substring_to_remove in original_string:
        updated_string = original_string.replace(substring_to_remove, "")
        return updated_string
    else:
        return original_string
    
def dezoom(driver, n):
    driver.set_context("chrome")
    win = driver.find_element(By.TAG_NAME,"html")
    for i in range(n):
        win.send_keys(Keys.CONTROL + "-")
    driver.set_context("content")

def zoom(driver, n):
    driver.set_context("chrome")
    win = driver.find_element(By.TAG_NAME,"html")
    for i in range(n):
        win.send_keys(Keys.CONTROL + "+")
    driver.set_context("content")

def scroll(driver, n):
    driver.execute_script("window.scrollTo(0, {i})".replace("{i}", str(n)))
    sleep(3)
    
def get_nbr(string):
    string = string.replace(",", ".")
    return int(re.sub("[^0-9]", "", string))

def get_nbr_comma(string):
    string = string.replace(",", ".")
    match = re.search(r"\d+", string)
    if match:
        return int(match.group())
    else:
        return None

def find_numbers_in_string(input_string):
    numbers = re.findall(r'\d+(?:\s*\d+)*', input_string)
    return numbers

def get_first_number(text):
    pattern = r'\d+'
    match = re.search(pattern, text)
    
    if match:
        first_number = match.group()
        return int(first_number)
    else:
        return None

def get_percentage(current, wanted):
    return (100 * (int(current) / int(wanted)))

def remove_spaces(string):
    return re.sub(r'\s+', ' ', string)

#get_create_sheet pour créer un nouvel onglet 
def verif_GS(df, URL, index):
    title = get_title_sheet(URL)
    onglets = get_list_sheet_name(URL)
    old_df = read_google_sheet(URL, onglets[index])
    
    print(f"Title: {title}, onglet: {onglets[index]}")
    concat_df = pd.concat([old_df, df])
    print(concat_df)
    write_google_sheet(concat_df.dropna(how='all'), URL, onglets[index])
    
    
#get_create_sheet pour créer un nouvel onglet 
def write_GS(df, URL, index):
    title = get_title_sheet(URL)
    onglets = get_list_sheet_name(URL)
    
    print(f"Title: {title}, onglet: {onglets[index]}")
    write_google_sheet(df.dropna(how='all'), URL, onglets[index])

"""body, tag, ttype, name"""
def find_single(body, tag, ttype, name):
    try:
        dest = make_printable(body.find(tag, {ttype:name}).text.strip())
    except:
        dest = None
        pass
    return dest

def find_all(body, tag, ttype, name):
    try:
        dest = body.find_all(tag, {ttype:name})
    except Exception as e:
        print(e)
        dest = None
        pass
    return dest

def find_link_by_class(soup, tag, ttype, name):
    elem = soup.find(tag, {ttype:name})

    if elem:
        return elem.get('href')

    return None

def find_all_link_by_class(soup, tag, ttype, name):
    elem = soup.find_all(tag, {ttype: name})
    myList = []

    for links in elem:
        myList.append(links.get('href'))

    return myList

def remove_substring_reg(main_string, substring):
    result_string = re.sub(re.escape(substring), '', main_string)
    return result_string

def get_database(str):
    client = MongoClient("localhost", 27017)
    if (str not in client.list_database_names()):
        exit(-1)
 
    return client[str]

def get_collection_list(db):
    print(db.list_collection_names())
    
def insert_data(collection, query):
    return (collection.insert_one(query))

def insert_list(collection, query_list):
    return (collection.insert_many(query_list))

def df_to_db(df, collection):
    collection.insert_many(df.to_dict('records'))
    
def df_to_airtable(df, table):
    df.fillna("No value", inplace=True)

    dico_table = df.to_dict('records')
    table.batch_create(dico_table)

""" Obtenir le tableau de la Airtable avec l'app id et le tblid (dans l'URL, appID/tblID)"""
def get_airtable(app_id, tbl_id):
    api = Api(os.environ['AIRTABLE_TOKEN'])
    
    return api.table(app_id, tbl_id)

""" Update une case d'une table airtable (l'id commençant par rec) """
def update_airtable_cell(table, rec_id, cell, content):
    table.update(rec_id, {cell: content})
    
def find_in_col(collection, query):
    for x in collection.find({}, query):
        print(x)
        
def get_collection_df(collection):
    try:
        df = pd.DataFrame(list(collection.find({})))
    except:
        df = pd.DataFrame()
        pass
    return df

def check_src_in_df(df, source):
    try:
        for index, line in df.iterrows():
            if (line["URL source"] == source):
                return index, line
    except:
        pass
    return None, None

def is_source_in_df(df, source):
    try:
        for index, line in df.iterrows():
            if (line["URL source"] == source):
                return index, line
    except:
        pass
    return pd.NA, pd.NA

def is_finished(old_df, driver, COLUMNS):
    i, l = is_source_in_df(old_df, driver.current_url)
    try:
        if not pd.isna(i):
            if l[2] and l[7]:
                if ("Echec" in l[2]) or ("Succès" in l[2] and "Terminée" in l[7]) or ("0 jour(s) restant(s)" in l[7]) or ("PROJET FINANCÉ" in l[4]) or "Réussi" in l[7]:
                    print("Skipped !")
                    myList = l.tolist()
                    myList.pop(0)
                    return dict(zip(COLUMNS, myList))
    except:
        pass
    return {}
