####
# This script uses Tableau Server Client to delete workbooks
# stored in a defined project. It will find a workbook that 
# matches a given project ID and delete the workbook.
# 
# This purpose of this sript is to be used as a part of a 
# systematic cleanup of reports by deleting reports that
# have been moved to a specified folder. 
#
# To run the script, you must have installed Python 3.7 or later.
# Before running the script, you are required to configure of the following:
# 'server address':      The address of the server holding the workbooks,
#                        where the server address has no trailing slash 
#                        (e.g. http://localhost);
# 'server':              where the tableau server is hosted 
# 'site_name':           The site, as listed in the sserver url 
# 'username':            the username of the account with appropriate server access;
# 'Password':            The password for the user;
# 'Archive project': The folder where the workbooks will be moved to.
# How these variables are configured is described below. 
####
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
archive_project = config.conf["project"]
password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site_name"]
username = config.conf["username"]


def main(server_url, site_name, username, password, archive_project):
    """
    Description:
        Deletes workbooks stored in the "archive_project" project
    Args:
        'server_url'      specified server address
        'site_name'       The name of the site that the user is signed into
        'username'        The username of the account authorising the request 
        'password'        The password of the account authorising the request
        'archive_project' The name of the project to move workbook into
        'workbook_list'   The list of the workbooks to move
    Returns:
        None
    """
    # Step 1: Sign in to server
    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        server.workbooks.all()
        try:
            workbooks = workbooks.filter(project_name=archive_project)
        except IndexError:
            raise LookupError(f"No project named {archive_project} found")

        # Step 4: Update workbook with new project id
        for i in len(workbooks): 
            workbook = workbooks[i]
            try:
                server.workbooks.delete(workbook.id)
            except IndexError:
                raise LookupError(f"No project named {workbook.id} found")

if __name__ == "__main__":
    main(server_url, site_name, username, password, archive_project, workbook_list)