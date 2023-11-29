from bs4 import BeautifulSoup
import pandas as pd
import requests

from crowd_utils import find_single, find_all_link_by_class, get_database, df_to_db, df_to_airtable, get_airtable, get_collection_df, check_src_in_df, update_airtable_cell

""" USEFUL CONSTANTS """
URL = "https://www.irena.org/News"
COLUMNS = ["Titre", "Body", "Date de création", "URL source", "ID_airtable"]

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
        title = find_single(soup, "h1", "class", "m-TopHeader__heading")
        body = find_single(soup, "div", "data-jscomponent", "RichText")
        creation_date = find_single(soup, "time", "class", "m-TopHeader__date")
                
        dico["Titre"] = title
        dico["Body"] = body
        dico["Date de création"] = creation_date
        
        dico["URL source"] = url
        dico_air = table.create(dico) # Airtable
        dico["ID_airtable"] = dico_air['id']
        collection.insert_one(dico) # Mongo
        update_airtable_cell(table, dico["ID_airtable"], "ID_airtable", dico["ID_airtable"])
        return stock_company_data(df, dico)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return stock_company_data(df, dico)

def scraping_irena():
    df = pd.DataFrame(columns = COLUMNS)
    db = get_database("Classification_articles")
    collection = db["Irena"]
    collection.delete_many({})
    old_df = get_collection_df(collection)
    
    table = get_airtable('appHREIqHIs32toy0', 'tblBtE2Zm7mmF7lSm')
    
    res = requests.get(URL)
    s = BeautifulSoup(res.text, 'html.parser')
    links = find_all_link_by_class(s, "a", "class", "c-BoxRelatedContentItem__heading")
    
    for company_url in links:
        full_url = URL.replace("/News", "") + company_url
        
        i, l = check_src_in_df(old_df, full_url)
        if i != None:
            print(f"Found it on index: {i}")
            continue

        df = get_company_data(full_url, table, collection, df)
    
    df = df.dropna()
    # df_to_db(df, collection)
    # df_to_airtable(df, table)
    return df

if __name__ == "__main__":
    df = scraping_irena()

    df.to_csv("./scraping_irena.csv", index=False)
    print("scraping finished")