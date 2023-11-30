from bs4 import BeautifulSoup
import pandas as pd
import requests

from crowd_utils import find_single, make_printable, find_all, find_all_link_by_class, get_database, df_to_db, get_airtable, get_collection_df, check_src_in_df, update_airtable_cell

""" USEFUL CONSTANTS """
URL = "https://energie-partagee.org/decouvrir/avec-nous/espace-presse/page/PAGE_ID/"
COOKIE_ID = "/html/body/div[1]/div/div[6]/button[1]"
COLUMNS = ["Titre", "Description", "Body", "Date de création", "URL source", "ID_airtable", "Résumé"]

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
        title = find_single(soup, "h1", "class", "t-title")
        body = find_single(soup, "div", "class", "fc")
        description = find_single(soup, "h2", "class", "t-excerpt")
        creation_date = find_all(soup, "div", "class", "c-tag -cartouche")
                
        dico["Titre"] = title
        dico["Description"] = description
        dico["Body"] = body
        if creation_date[1]:
            dico["Date de création"] = make_printable(creation_date[1].text.strip())
        
        dico["URL source"] = url
        dico_air = table.create(dico) # Airtable
        dico["ID_airtable"] = dico_air['id']
        collection.insert_one(dico) # Mongo
        update_airtable_cell(table, dico["ID_airtable"], "ID_airtable", dico["ID_airtable"])
        
        dico["Résumé"] = "Temporary value"
        update_airtable_cell(table, dico["ID_airtable"], "Résumé", dico["Résumé"])
        return stock_company_data(df, dico)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return stock_company_data(df, dico)

def scraping_energie_partagee():
    df = pd.DataFrame(columns = COLUMNS)
    db = get_database("Classification_articles")
    collection = db["energie_partagee"]
    collection.delete_many({}) ## to_comment
    
    old_df = get_collection_df(collection)
    table = get_airtable('appHREIqHIs32toy0', 'tblzWMjPtqbsDWlm5')
    
    i = 1
    res = requests.get(URL.replace("PAGE_ID", str(i)))
    s = BeautifulSoup(res.text, 'html.parser')
    links = []
    tmp = []
    while True:
        tmp = find_all_link_by_class(s, "a", "target", "_self")
        if not tmp:
            break
        links += tmp
        tmp = []
        i += 1
        try:
            res = requests.get(URL.replace("PAGE_ID", str(i)))
            s = BeautifulSoup(res.text, 'html.parser')
        except:
            break
    
    for company_url in links:
        i, l = check_src_in_df(old_df, company_url)
        if i != None:
            print(f"Found it on index: {i}")
            continue

        df = get_company_data(company_url, table, collection, df)
    
    # df_to_db(df, collection)
    return df

if __name__ == "__main__":
    #df = pd.DataFrame(columns = COLUMNS)
    df = scraping_energie_partagee()

    df.to_csv("./CSV/scraping_energie_partagee.csv", index=False)
    print("scraping finished")