from scraping_energie_partagee import scraping_energie_partagee
from scraping_irena import scraping_irena
from scraping_neozone import scraping_neozone
from transformers import pipeline
import re

import pandas as pd
from crowd_utils import get_database, df_to_db, df_to_airtable, get_airtable, update_airtable_cell

def cut_string_into_slices(input_string):
    slice_length = 1024
    slices = [input_string[i:i + slice_length] for i in range(0, len(input_string), slice_length)]
    return slices

replace_words = {
    "is": "est",
    "of": "de",
    "and": "et",
    "the": "le",
    "to": "à",
    "if": "si",
}

def replace_words_in_text(text, replace_words):
    for word, replacement in replace_words.items():
        pattern = re.compile(r"\b" + word + r"\b")
        text = pattern.sub(replacement, text)
    return text

if __name__ == '__main__':
    L = []
    try:
        L.append(scraping_energie_partagee())
    except:
        pass
    try:
        L.append(scraping_irena())
    except:
        pass
    try:
        L.append(scraping_neozone())
    except:
        pass
    df = pd.concat(L)

    """ END OF SCRAPING, INPUTING DF INTO AIRTABLE AND MONGO"""
    table = get_airtable('appHREIqHIs32toy0', 'tblvM3eejH1sjlraT')
    df_to_airtable(df, table)
    # df.to_csv("All_articles.csv", index=False)
    
    db = get_database("Classification_articles")
    collection = db["All articles"]
    collection.delete_many({})
    
    try:
        df_to_db(df, collection)
    except:
        print("No new articles to send.")

    """ SUMMARIES """
    model_name = "facebook/bart-large-cnn"
    summarizer = pipeline('summarization', model=model_name)
    
    summary_list = df['Body'].tolist()
    row_id_list = df['ID_airtable'].tolist()
    titre_list = df['Titre'].tolist()
    URL_list = df['URL source'].tolist()
    all_summaries = []
    
    for i, article in enumerate(summary_list):
        article_sliced = cut_string_into_slices(article)
        nb_times = 2
        article_summary = ""
    
        for slice_text in article_sliced:
            if nb_times <= 0:
                break
    
            summary_text = summarizer(slice_text)
            article_summary += summary_text[0]['summary_text']
    
            nb_times -= 1
        
        article_summary = replace_words_in_text(article_summary, replace_words)
        print(f"INDEX: {i}\nNew summary: {article_summary}"), print("")
        all_summaries.append({'Titre': titre_list[i], 'summary_text': article_summary,
                              'URL source': URL_list[i], 'ID_airtable': row_id_list[i]})

        update_airtable_cell(table, row_id_list[i], "Résumé", article_summary) ## Update airtable's cell
    
    circle_df = pd.DataFrame(all_summaries)
    circle_df.to_csv("./Résumé_articles.csv", index=False)
    ##NE PAS OUBLIER DE RETIRER LES COLLECTION.delete_many({})
    
    ## mettre les summaries dans la section résumé

    ## Il faut récupérer les df qui stockent aussi dans mongoDB tous les articles, ensuite on résume les articles et on envoie cela sur Airtable, un status sera ensuite assigné à la main pour dire s'il est validé ou non.
    ## Pour ça, j'ai besoin de l'id des cases pour pouvoir vérifier s'il y a un résumé, s'il n'y en a pas, le faire, l'envoyer sur Airtable et voilà. Ensuite quand le résumé sera validé, il faudra voir pour le récupérer, l'envoyer sur Circle sous la forme Titre de base + Résumé + URL de l'article de base + (Optionel) tags