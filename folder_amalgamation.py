import tableauserverclient as TSC
import conf as config


password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
to_folder = config.conf["to_folder"]
from_folder = "to be delete"


def main(server_url, site_name, username, password, from_folder, to_folder):
    """
    Description:
        Moves workbooks listed in the config file to the "Archive" project
    Args:
        'server_url'      specified server address
        'site_name'       The name of the site that the user is signed into
        'username'        The username of the account authorising the request 
        'password'        The password of the account authorising the request
        'to_folder' The name of the project to move workbook into
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
            dest_project = server.projects.filter(name=to_folder)[0]
        except IndexError:
            raise LookupError(f"No project named {to_folder} found.")

        # Step 2: Find from project
        try:
            from_project = server.projects.filter(name=from_folder)[0]
        except IndexError:
            raise LookupError(f"No project named {from_folder} found.")

        # Step 3: more each workbook
        for workbook in TSC.Pager(server.workbooks):
            
            if workbook.project_id == from_project.id:

                workbook.project_id = dest_project.id
                server.workbooks.update(workbook)

if __name__ == "__main__":
    main(server_url, site_name, username, password, from_folder, to_folder)