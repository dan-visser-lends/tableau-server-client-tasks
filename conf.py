import pandas as pd 
"""
    conf.py is the dictionary of all the credentials required 
    to access tableau server through the a API, as well as 
    the list of workbooks to be moved on the server.
"""
conf = {
    "API_version": "3.4",
    "username": "username.co.uk", 
    "password": "password",
    "server": "something.co.uk",
    "site": "",
    "project": "Archive Project", 
    "workbooks": pd.read_csv("low_importance_reports.csv")["Workbook Name (expand for sheets)"].to_list()
}    


