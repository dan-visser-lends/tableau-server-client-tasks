from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying
import tableauserverclient as TSC
import pandas as pd
import conf as config

password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
archive_project = config.conf["project"]

api_conf = { 
    "api_config": {
        "server": "some_server.com",
        "api_version": "3.10",
        "personal_access_token_name": "some-token", 
        "personal_access_token_secret": "some-token-id",
        "site_name": "",
        "site_url" : ""
    }
}   

def extract_tasks(api_conf):

    conn = TableauServerConnection(api_conf, env="api_config")
    conn.sign_in()
    
    schedulesdf= querying.get_schedules_dataframe(conn)
    schedule_list = schedulesdf[schedulesdf["type"]=="Extract"].id.to_list()

    extract_task_list = []

    for schedule in schedule_list: 

        extract_tasks = conn.get_extract_refresh_tasks_for_schedule(
            schedule_id=schedule
            ).json()

        extract_task_list.append(extract_tasks)

    return extract_task_list


def delete_archived_workbook_tasks(server_url, site_name, username, password, archive_project, extract_task_list):
    
    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        try:
            archive_project = server.projects.filter(name=archive_project)[0]
        except IndexError:
            raise LookupError(f"No project named {archive_project} found.")

        archived_id_list = []
        for wb in TSC.Pager(server.workbooks):

            if wb.project_id == archive_project.id:
                archived_id_list.append(wb.id)

        list_of_extracts = []

        for extract_task in extract_task_list:

            if "extract" in extract_task["extracts"]:   
                    
                extracts_list = extract_task["extracts"]["extract"]

                for extract in extracts_list:

                    if 'workbook' in extract.keys():

                        if extract["workbook"]["id"] in archived_id_list:

                            list_of_extracts.append(extract["workbook"]["id"])

        print(len(list_of_extracts))


if __name__ == "__main__":

    extract_task_list = extract_tasks(api_conf)

    delete_archived_workbook_tasks(server_url, site_name, username, password, archive_project, extract_task_list)



