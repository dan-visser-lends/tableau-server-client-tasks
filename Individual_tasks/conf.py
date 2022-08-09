import pandas as pd 
"""
    conf.py is the dictionary of all the credentials required 
    to access tableau server through the a API, as well as 
    the list of workbooks to be moved on the server.
"""
conf = {
    "API_version": "3.4",
    "username": "daniel.visser@lendingworks.co.uk", 
    "password": "pbf9kqw9rqk!rtx3UAN",
    "server": "https://tableau.lendingworks.co.uk",
    "site": "",
    "project": "Archive Project", 
    "to_folder": "",
    "workbooks": ""
}    

api_conf = { 
    "api_config": {
        "server": "https://tableau.lendingworks.co.uk/",
        "api_version": "3.10",
        "personal_access_token_name": "dans-token", 
        "personal_access_token_secret": "yhLpL7H+ROS6pxaIU6hx8Q==:HXjYoexRjeM6n9ekOVsDYwHXhp88hbJz",
        "site_name": "",
        "site_url" : ""
    }
}   


