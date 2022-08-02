####
# This script uses Tableau Server Client to move a workbook 
# from one project to another. It will find a workbook that 
# matches a given name and update it to be in the desired project.
# 
# The prupose of this script is to systematically add workbooks 
# that are no longer being used to be archived.
#
# To run the script, you must have installed Python 3.7 or later.
# Before running the script, you are required to configure of the following:
# 'server address':      the address where the tableau server is hosted;
# 'site_name':           the site, as listed in the server url;
# 'username':            the username of the account with appropriate server access;
# 'Password':            The password for the user;
# 'Workbooks':           The name of the workbooks to be moved;
# 'Destination project': The project where the workbooks will be moved to.
# 
# How these variables are configured is described below. 
####

import tableauserverclient as TSC
import conf as config

### IMPORTING THE CONFIG VARIABLES ### 
# The conf.py file requies a dictionary of key-value pairs, labelled:
#   "project",
#   "password",
#   "server",
#   "site_name",
#   "username",
#   "workbooks". 
# Note, the value of the key "workbooks" is itself a list of workbook names.
archive_project = config.conf["project"]
password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
workbook_to_move = config.conf["workbooks"]


def main(server_url, site_name, username, password, archive_project, workbook_to_move):
    """
    Description:
        Moves workbooks listed in the config file to the "Archive" project
    Args:
        'server_url'      specified server address
        'site_name'       The name of the site that the user is signed into
        'username'        The username of the account authorising the request 
        'password'        The password of the account authorising the request
        'archive_project' The name of the project to move workbook into
        'workbook_to_move'   The list of the workbooks to move
    Returns:
        None
    """
    # Step 1: Sign in to server
    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        # Step 2: Find destination project
        try:
            dest_project = server.projects.filter(name=archive_project)[0]
        except IndexError:
            raise LookupError(f"No project named {archive_project} found.")

        # Step 3: list all workbooks, both on the server and already in the archive folder.
        all_workbooks_list = []
        archive_list = []
        for wb in TSC.Pager(server.workbooks):
            all_workbooks_list.append(wb.name)

            if wb.project_id == dest_project.id:
                archive_list.append(wb.name)

        # Step 4: Filter the "to be archived" workbooks by those that are actually on the server
        intersection = set(all_workbooks_list).intersection(workbook_to_move)
        workbook_list = list(intersection)
        print(len(workbook_list))
        print(len(archive_list))


        # Step 5: Archive each workbook
        for workbook in TSC.Pager(server.workbooks):
            
            if workbook.name in workbook_list:

                if workbook.project_id != dest_project.id:
                    workbook.project_id = dest_project.id

                    if workbook.name in archive_list: 
                        workbook.name = workbook.name + "_2"
                        archive_list.append(workbook.name)
                        server.workbooks.update(workbook)

                    else: 
                        archive_list.append(workbook.name)
                        server.workbooks.update(workbook)

if __name__ == "__main__":
    main(server_url, site_name, username, password, archive_project, workbook_to_move)
   