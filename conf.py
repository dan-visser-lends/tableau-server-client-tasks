import pandas as pd 
"""
    conf.py is the dictionary of all the credentials required 
    to access tableau server through the a API, as well as 
    the list of workbooks ot be moved on the server.
"""
conf = {
    "API_version": "3.4",
    "project": "something", 
    "password": "password",
    "server": "server",
    "username": "username", 
    "workbooks": pd.read_csv("lw_low_importance_tableau_repoerts.csv")["Workbook Name (expand for sheets)"].to_list()
}    

