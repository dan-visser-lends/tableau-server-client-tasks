import tableauserverclient as TSC
from tableau_api_lib.utils import querying
import psycopg2 
import pandas as pd
import logging
import conf as config 

api_conf = { 
    "api_config": {
        "server": "something",
        "api_version": "3.10",
        "personal_access_token_name": "dans-token", 
        "personal_access_token_secret": "password",
        "site_name": "",
        "site_url" : ""
    }
}   

password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
archive_project = config.conf["project"]


def active_workbooks():
    
    logger = logging.getLogger()
    
    conn = None
    try:
        conn = psycopg2.connect(host="someserver.co.uk",
                                port=8060,
                                database="workgroup",
                                user="readonly",
                                password="some password")

        curr = conn.cursor()

        query = "\
            SELECT\
                last_view_time,\
                views_workbook_id\
            FROM\
                public._views_stats\
            WHERE\
                last_view_time > CURRENT_DATE - INTERVAL '3 months';"

        curr.execute(query)
        conn.commit()

        df = pd.DataFrame(curr.fetchall(), columns=[desc[0] for desc in curr.description])
        active_workbooks = df["views_workbook_id"].to_list()

    except Exception as error: 
        logger.error(f"Error while connecting to server repositry. {error}")
        logger.error(error, exc_info=True)

    finally:
        if conn:  
            curr.close()
            conn.close()

    return str(active_workbooks)


def archive_workbooks(server_url, site_name, username, password, archive_project, active_workbooks):

    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        try:
            dest_project = server.projects.filter(name=archive_project)[0]
        except IndexError:
            raise LookupError(f"No project named {archive_project} found.")
        
        archive_list = []
        for wb in TSC.Pager(server.workbooks):

            if wb.project_id == dest_project.id:
                archive_list.append(wb.name)

        check_list = []

        for wb in TSC.Pager(server.workbooks):
            url_id = wb._webpage_url.split("workbooks/")[1]

            if url_id not in active_workbooks: 

                if wb.project_id != dest_project.id:
                    wb.project_id = dest_project.id

                    if wb.name in archive_list: 
                        wb.name = wb.name + "_2"
                        archive_list.append(wb.name)
                #         server.workbooks.update(wb)

                    else: 
                        archive_list.append(wb.name)
                #         server.workbooks.update(wb)

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
            dest_project = server.projects.filter(name=archive_project)[0]
        except IndexError:
            raise LookupError(f"No project named {archive_project} found.")

        archived_id_list = []
        for wb in TSC.Pager(server.workbooks):

            if wb.project_id == dest_project.id:
                archived_id_list.append(wb.id)

        for extract_task in extract_task_list:

            if "extract" in extract_task["extracts"]:   
                    
                extracts_list = extract_task["extracts"]["extract"]

                for extract in extracts_list:

                    if 'workbook' in extract.keys():

                        if extract["workbook"]["id"] in archived_id_list:

                            server.tasks.delete(extract["id"])


def main(server_url, site_name, username, password, archive_project, api_conf): 
    
    ##### STEP 1: Find and archive stale workbooks #####
    stale_workbooks = active_workbooks()
    archive_workbooks(server_url, site_name, username, password, archive_project, stale_workbooks)
    
    ##### STEP 2: Remove extract refresh tasks from archived workbooks  #####
    extract_task_list = extract_tasks(api_conf)
    delete_archived_workbook_tasks(server_url, site_name, username, password, archive_project, extract_task_list)
    

if __name__ == "__main__":

    main()
