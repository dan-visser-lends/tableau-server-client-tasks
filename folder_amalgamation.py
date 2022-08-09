### AMALGAMATE FOLDERS 
# This script is built to move all workbooks inside a project
# to another project. The script works by matching workbooks whos
# project ID matches the project ID of the "from project" and, 
# using the project ID, moves it to the "to project". 
# 
# The limitation of this process is that it does not move workbooks
# inside sub-projects of the "from project". This choice is intentional.
# If sub-folders also need to be almalgamates, the script can be 
# run again using the sub-project ID for the "from project". 
# 
# To run the script, you must have installed Python 3.7 or later, as 
# we as tableauserverclient. The script is currently set up to pull 
# from a config file that lives in the same directory as the script 
# running. The config files is a .py file with a python dictionary 
# of config variables.

import tableauserverclient as TSC
import conf as config


password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
from_project = config.conf["to_folder"]
to_project = config.conf["to_folder"]


def main(server_url, site_name, username, password, from_project, to_project):
    """
    Description:
        Moves all workbooks from one project to another
    Args:
        'server_url'      specified server address
        'site_name'       The name of the site that the user is signed into
        'username'        The username of the account authorising the request 
        'password'        The password of the account authorising the request
        'from_project'    The project 
        'to_folder'       The project to move the workbooks into 
    Returns:
        None
    """
    # Step 1: Sign in to server
    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        # Step 2: Find destination project
        try:
            dest_project = server.projects.filter(name=to_project)[0]
        except IndexError:
            raise LookupError(f"No project named {to_project} found.")

        # Step 2: Find from project
        try:
            from_project = server.projects.filter(name=from_project)[0]
        except IndexError:
            raise LookupError(f"No project named {from_project} found.")

        # Step 3: more each workbook
        for workbook in TSC.Pager(server.workbooks):
            
            if workbook.project_id == from_project.id:

                workbook.project_id = dest_project.id
                server.workbooks.update(workbook)

if __name__ == "__main__":
    main(server_url, site_name, username, password, from_project, to_project)