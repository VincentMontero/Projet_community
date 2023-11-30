from bs4 import BeautifulSoup
import pandas as pd
import requests
from datetime import datetime
import locale

from crowd_utils import find_single, find_all_link_by_class, get_first_number, get_database, df_to_db, get_airtable, get_collection_df, check_src_in_df, update_airtable_cell


""" USEFUL CONSTANTS """
URL = "https://www.neozone.org/"
COLUMNS = ["Titre", "Description", "Body", "Date de création", "URL source", "ID_airtable"]

""" Get wanted data from the dictionnary and insert it in the dataframe """
def stock_company_data(df, dico):
    tmp = [dico[key] for key in COLUMNS]

    df.loc[len(df.index)] = tmp
    # print(f"LISTE = {tmp}")
    return df

def get_company_data(url, table, collection, df):
    dico = dict.fromkeys(COLUMNS)
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        title = find_single(soup, "h1", "class", "post-title entry-title")
        description = find_single(soup, "h2", "class", "entry-sub-title")
        body = find_single(soup, "div", "class", "entry-content entry clearfix")
        
        creation_date = find_single(soup, "span", "class", "date meta-item tie-icon")
        locale.setlocale(locale.LC_TIME, 'fr_FR') # obligé à mettre pour la ligne d'après
        given_date = datetime.strptime(creation_date, '%d %B %Y')
        current_date = datetime.now()
        
        dif = get_first_number(str(given_date - current_date))
        
        if dif > 7:
            return stock_company_data(df, dico)
                
        dico["Titre"] = title
        dico["Description"] = description
        dico["Body"] = body
        dico["Date de création"] = creation_date
        
        dico["URL source"] = url
        if dico:
            dico_air = table.create(dico) # Airtable
            dico["ID_airtable"] = dico_air['id']
            update_airtable_cell(table, dico["ID_airtable"], "ID_airtable", dico["ID_airtable"])
            collection.insert_one(dico) # Mongo
        dico["Résumé"] = "Temporary value"
        update_airtable_cell(table, dico["ID_airtable"], "Résumé", dico["Résumé"])
        return stock_company_data(df, dico)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return stock_company_data(df, dico)

def scraping_neozone():
    df = pd.DataFrame(columns = COLUMNS)
    db = get_database("Classification_articles")
    collection = db["Neozone"]
    collection.delete_many({}) ## to_comment

    old_df = get_collection_df(collection)
    
    table = get_airtable('appHREIqHIs32toy0', 'tblcn9Ky6s37tQi7t')

    res = requests.get(URL)
    s = BeautifulSoup(res.content, 'html.parser')
    links = find_all_link_by_class(s, "a", "class", "all-over-thumb-link")
    links += find_all_link_by_class(s, "a", "class", "post-thumb")

    for company_url in links:
        i, l = check_src_in_df(old_df, company_url)
        if i != None:
            print(f"Found it on index: {i}")
            continue
        df = get_company_data(company_url, table, collection, df)
    
    df = df.dropna()
    # df_to_db(df, collection)
    return df

if __name__ == "__main__":
    df = scraping_neozone()
    ##table.all() = liste de dictionnaires
    ##table.update('recDzhHBQDgDJk4kl', {'Résumé': 'Test'})
    df.to_csv("./CSV/scraping_neozone.csv", index=False)
    print("scraping finished")