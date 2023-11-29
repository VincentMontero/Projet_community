# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 14:13:09 2021

@author: Collabcap
"""

import gspread
from gspread_dataframe import (get_as_dataframe,
                               set_with_dataframe)
import gspread_formatting
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
import time

import httplib2
from apiclient import discovery

import string

import datetime

###########################################################################

scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name("./credentials.json", scope)

gc = gspread.authorize(credentials)

###################################################################



def n2a(n,b=string.ascii_uppercase):
   d, m = divmod(n,len(b))
   return n2a(d-1,b)+b[m] if d else b[m]

############################################

def get_create_sheet(url, name_sheet) :
    """
    Génère un onglet avec ce nom sur ce GS, si c'est possible

    Parameters
    ----------
    url : str
    name_sheet : str

    Returns
    -------
    None.

    """
    time.sleep(1)
    x = gc.open_by_url( url)
    x.add_worksheet(title = name_sheet , rows= 20, cols=10)
    

def get_title_sheet(url):
    """
    Donne le titre de ce GS

    Parameters
    ----------
    url : str
        url d'un GS, dont j'ai accès.

    Returns
    -------
    str

    """
    time.sleep(1)
    x = gc.open_by_url( url) 
    return( x.title )

def get_list_sheet_name(url) :
    """
    Liste le nom des onglets de ce GS

    Parameters
    ----------
    url : str
        url d'un GS, dont j'ai accès.

    Returns
    -------
    List of str.

    """
    x = gc.open_by_url( url)  
    try :
        list_brut = x.worksheets()
    except :
        time.sleep(30)
        list_brut = x.worksheets()
    list_title = [ i.title for i in  list_brut ]
    return( list_title )



def read_google_sheet( url , sheet_name ) :
    """
    Lit l'onglet d'un GS demandé, sous forme d'un pandas DataFrame

    Parameters
    ----------
    url : str
        
    sheet_name : url
        

    Returns
    -------
    pandas.core.frame.DataFrame.

    """
    #sheet_name = get_title_sheet(url)
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    
    wb = gc.open_by_key(key_sheet)
    sheet = wb.worksheet(sheet_name)
    return( get_as_dataframe(sheet, parse_dates=True, ) )

def read_google_sheet_with_formulas( url , sheet_name ) :
    """
    Lit l'onglet d'un GS demandé, sous forme d'un pandas DataFrame

    Parameters
    ----------
    url : str
        
    sheet_name : url
        

    Returns
    -------
    pandas.core.frame.DataFrame.

    """
    #sheet_name = get_title_sheet(url)
    try :
        key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
        
        wb = gc.open_by_key(key_sheet)
        sheet = wb.worksheet(sheet_name)
        return( get_as_dataframe(sheet, parse_dates=True, evaluate_formulas=True) )
    except :
        time.sleep(30)
        key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
        
        wb = gc.open_by_key(key_sheet)
        sheet = wb.worksheet(sheet_name)
        return( get_as_dataframe(sheet, parse_dates=True, evaluate_formulas=True) )

def read_google_sheet_by_id( url ) :
    
    #sheet_name = get_title_sheet(url)
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0].strip()
    
    wb = gc.open_by_key(key_sheet)
    
    try :
        id_sheet = int( url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[1].split("gid=")[1] )
        
        sheet_name = [ item.title for item in wb.worksheets() if item.id == id_sheet ][0]
        
        time.sleep( 5 )
        
        df = read_google_sheet( url , sheet_name ) 
        return( {"statut" : "Valide", "df" : df, "sheet_name" : sheet_name, "spreadsheet_name" : wb.title } )
    except :
        return( {"statut" : "Erreur", "df" : pd.DataFrame(), "sheet_name" : "" , "spreadsheet_name" : ""} )
        

def get_id_sheet( url , sheet_name ) :
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    
    wb = gc.open_by_key(key_sheet)
    sheet = wb.worksheet(sheet_name)
    return(  sheet.id )

def title_google_spreadsheet( url ) :
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    
    wb = gc.open_by_key(key_sheet)
    return( wb.title )

def write_google_spreadsheet( sheet, df ) :
    set_with_dataframe(sheet, df,
                     include_index=False,
                     resize=True,)

def write_google_sheet(df, url, sheet_name ) :
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    wb = gc.open_by_key(key_sheet)
    try :
        write_google_spreadsheet(wb.worksheet(sheet_name), df )
    except :
        time.sleep(30)
        write_google_spreadsheet(wb.worksheet(sheet_name), df )
    try :
        nb_row = df.shape[0]
        gspread_formatting.set_row_height( wb.worksheet(sheet_name), "2:"+str( int(nb_row+1)) , 21 )
    except :
        pass
    
def write_google_sheet_sans_format_taille(df, url, sheet_name ) :
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    wb = gc.open_by_key(key_sheet)
    try :
        write_google_spreadsheet(wb.worksheet(sheet_name), df )
    except :
        time.sleep(30)
        write_google_spreadsheet(wb.worksheet(sheet_name), df )
    
def write_google_sheet_by_id(df, url ) :
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    wb = gc.open_by_key(key_sheet)
    
    id_sheet = int( url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[1].split("gid=")[1] )
    try :
        time.sleep( 5)
        sheet_name = [ item.title for item in wb.worksheets() if item.id == id_sheet ][0]
        write_google_sheet(df, url, sheet_name )
        #write_google_spreadsheet( wb.get_worksheet( id_sheet ), df )
    except :
        time.sleep(30)
        #write_google_spreadsheet( wb.get_worksheet( id_sheet ), df )
        sheet_name = [ item.title for item in wb.worksheets() if item.id == id_sheet ][0]
        write_google_sheet(df, url, sheet_name )
    nb_row = df.shape[0]
    #gspread_formatting.set_row_height( wb.worksheet(sheet_name), "2:"+str( int(nb_row)) , 21 )

def get_folder_contents(folder_id):
    http = credentials.authorize(httplib2.Http() )
    service = discovery.build('drive', 'v3' , http=http)

    results = service.files().list(
        q=("'{0}' in parents".format(folder_id)),
        corpora="user",
        fields="nextPageToken, files(id, name, webContentLink,createdTime, modifiedTime)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    return( items )

def get_folder_contents_bis(folder_id):
    http = credentials.authorize(httplib2.Http() )
    service = discovery.build('drive', 'v3' , http=http)

    results = service.files().list(
        q=("'{0}' in parents".format(folder_id)),
        corpora="user",
        fields="nextPageToken, files(id, name,  webViewLink,createdTime, modifiedTime)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    return( items )

def get_list_of_link_GS( folder_id) :
    time.sleep(5)
    list_out = []
    list_temp = get_folder_contents_bis(folder_id) 
    for elt in list_temp :
        try :
            if 'webViewLink' in elt.keys() :
                if "/spreadsheets/" in elt['webViewLink'] :
                    list_out.append({"nom" : elt["name"],
                                     "url" : elt ['webViewLink']})
                elif "/drive/folders/" in  elt['webViewLink'] : 
                    list_out = list_out + get_list_of_link_GS(elt["id"])
                else :
                    pass
            else :
                pass
        except :
            pass
    return( list_out )

url = "https://docs.google.com/spreadsheets/d/1rrJOBamsT4Jsqfg1-YijpYdI6eoCw6OAAC7zyvUeazQ/edit#gid=1464926788"
folder_id = "1ZXTEqQxEU8EWfiDzs34pjmNYrPfynzFY"


def drop_list( df, column_drop_list, column_sheet, url, sheet_name):
    """
    Ajout d'une colonne drop list à un GS

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        DataFrame correspondant au GS
    column_drop_list : list 
        liste des valeur de la drop list.
    column_sheet : str
        colonne du GS concernée.
    url : str
        url du GS.
    sheet_name : str
        onglet du GS concerné.

    Returns
    -------
    None.

    """
    validation_rule = gspread_formatting.DataValidationRule(
        gspread_formatting.BooleanCondition('ONE_OF_LIST', column_drop_list ),
        showCustomUi=True
    )
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    wb = gc.open_by_key(key_sheet)
    sheet = wb.worksheet(sheet_name)
    
    position = df.columns.tolist().index( column_sheet )
    column_excel = n2a(position)
    
    phrase = column_excel+"2:"+column_excel+str( df.shape[0] +1 )
    
    gspread_formatting.set_data_validation_for_cell_range(sheet, phrase, validation_rule)
    

def get_last_user_id_file( file_id ) :
    http = credentials.authorize(httplib2.Http() )
    service = discovery.build('drive', 'v3' , http=http)
    
    response = service.revisions().list(
        fileId=file_id,
        fields='*',
        pageSize=1000
    ).execute()

    revisions = response.get('revisions')
    nextPageToken = response.get('nextPageToken')

    count = 1
    while nextPageToken and count < 10:
        time.sleep( 2 )
        response = service.revisions().list(
            fileId=file_id,
            fields='*',
            pageSize=1000,
            pageToken=nextPageToken
        ).execute()

        revisions = response.get('revisions')
        nextPageToken = response.get('nextPageToken')
        count +=1 
        
    data = {
        "hour" : datetime.datetime.strptime( revisions[-1]["modifiedTime"] ,  "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%d/%m/%Y'),
        "user" : revisions[-1]["lastModifyingUser"]["displayName"],
        "me" : revisions[-1]["lastModifyingUser"]["me"],
        "valid" : True,
        }
    if data[ "user" ] == 'Axel DACALOR' :
        data.update({"me" : True})
    return( data )

def get_last_user_url_GS( url ) :
    try :
        file_id = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
        return( get_last_user_id_file( file_id )  )
    except :
        return( {"valid" : False, "me" : False})

def get_all_GS(url) :
    """
    Listing GS

    Parameters
    ----------
    url : str
        url GS.

    Returns
    -------
    List.

    """
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    L_out = []
    
    wb = gc.open_by_key(key_sheet)
    WS = wb.worksheets()
    time.sleep( 5 )
    
    Titre = wb.title 
    print("------------------------------------------------------------------------------------------------------------")
    print("Sourcing : {}".format(Titre))
    print("------------------------------------------------------------------------------------------------------------")
    
    for elt in WS :
        try :
            time.sleep(10)
            data = {
            "Titre GS" : Titre,
            "Onglet" : elt.title,
            "df" : get_as_dataframe(elt, parse_dates=True, )
            }
            print(data["Onglet"])
            L_out.append(data)
        except :
            try :
                time.sleep(40)
                data = {
                "Titre GS" : Titre,
                "Onglet" : elt.title,
                "df" : get_as_dataframe(elt, parse_dates=True, )
                }
                print(data["Onglet"])
                L_out.append(data)
            except :
                print("error onglet n'existe pas")
    time.sleep( 5 )
    print("Fin de sourcing : {}".format(Titre))
    print("-------------------------------------------")
    print("")
    return( L_out )

def get_all_GS_bis(url) :
    """
    Listing GS

    Parameters
    ----------
    url : str
        url GS.

    Returns
    -------
    List.

    """
    key_sheet = url.replace('https://docs.google.com/spreadsheets/d/','').split("/")[0]
    
    all_sheet_names = get_list_sheet_name(url) 
    time.sleep( 5 )
    Titre = get_title_sheet(url)
    L_out = []
    
    
    time.sleep( 5 )
    
    print("------------------------------------------------------------------------------------------------------------")
    print("Sourcing : {}".format(Titre))
    print("------------------------------------------------------------------------------------------------------------")
    
    for sheet_name in all_sheet_names:
        
        try :
            SHEET_ID = key_sheet
            SHEET_NAME = sheet_name.replace(" ","%20").replace("é","%c3%a9").replace("è","%c3%a8").replace("-","%2d").replace("_","%5f")
            url_sheet = 'https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'.format(SHEET_ID, SHEET_NAME)
            df =  pd.read_csv(url_sheet , encoding = 'utf8')
            if len( str( df.iloc[0] ) ) > 50e3 or max( [len(col for col in df.columns)] ) > 200 :
                time.sleep(5)
                df = read_google_sheet(url, sheet_name)
            else :
                pass
            data = {
                "Url GS" : url,
            "Titre GS" : Titre,
            "Onglet" : sheet_name,
            "df" : df
            }
            print(data["Onglet"])
            L_out.append(data)
        except : 
            if 'Feuille 1' == sheet_name :
                pass
            else :
                try :
                    print("error")
                    time.sleep(5)
                    df = read_google_sheet(url, sheet_name)
                    data = {
                        "Url GS" : url,
                    "Titre GS" : Titre,
                    "Onglet" : sheet_name,
                    "df" : df
                    }
                    print(data["Onglet"])
                    L_out.append(data)
                except :
                    print("error onglet n'existe pas")
    print("Fin de sourcing : {}".format(Titre))
    print("-------------------------------------------")
    print("")
    return( L_out )


#######################################################################################################################################################################

def create_GS( name_sheet,  folder_id  ) :
    """
    Génère un GS dans le répertoire choisi. Cependant, le propriétaire serait un compte service.
    Mais il peut etre supprimé. Le GS n'est pas publique, donc intervention manuelle.
    
    None,"anyone", "writer" --> tout le monde peut le trouver sur Google et le modifier
    None,"anyone", "reader" --> tout le monde peut le trouver sur Google sans le modifier
    None,"anyone", "writer",with_link=True --> tout le monde sui ont le lien peuvent modifier ce fichier
    
    Parameters
    ----------
    name_sheet : str
        DESCRIPTION.
    folder_id : str
        DESCRIPTION.
    owner_gmail_address : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    temp = gc.create (name_sheet, folder_id )
    time.sleep( 5 )
    rep = temp.share( None , "anyone", "writer"  , with_link=True)
    return( temp.url )
