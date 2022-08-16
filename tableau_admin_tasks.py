import tableauserverclient as TSC
from tableau_api_lib.utils import querying
import psycopg2 
import pandas as pd
import logging
import conf as config 
import os
import datetime
from datetime import timedelta 
from dateutil.relativedelta import relativedelta

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

password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]
archive_project = config.conf["project"]
version = config.conf["API_version"]


def workbook_classifier():
    
    logger = logging.getLogger()
    
    conn = None
    try:
        conn = psycopg2.connect(host="tableau.lendingworks.co.uk",
                                port=8060,
                                database="workgroup",
                                user="readonly",
                                password="HWWwZGmFCN7ikoN98hirG9kBbBVHpr66yHNfBqAmY6oW9DXkYy")

        curr = conn.cursor()

        query = "\
            SELECT\
                v.last_view_time,\
                v.views_workbook_id,\
                u.name\
            FROM\
                public._views_stats v\
            JOIN\
                public._users u\
            ON\
                u.id = v.users_id\
            WHERE\
                last_view_time > CURRENT_DATE - INTERVAL '4 months';"


        curr.execute(query)
        conn.commit()

        df = pd.DataFrame(curr.fetchall(), columns=[desc[0] for desc in curr.description])


        now = datetime.datetime.utcnow()
        active_limit = now - relativedelta(months=3)
        reminder_limit = now - relativedelta(months=3) + relativedelta(weeks=1)

        active_workbooks = df[(df["last_view_time"]>active_limit)]["views_workbook_id"].astype(str).to_list()
        to_be_archived_workbooks = df[(df["last_view_time"]<active_limit)]["views_workbook_id"].astype(str).to_list()
        reminder_workbooks = df[(df["last_view_time"]>active_limit)&(df["last_view_time"]<reminder_limit)]["views_workbook_id"].astype(str).to_list()

        reminder_workbooks = list(set(reminder_workbooks) - set(active_workbooks))
        to_be_archived_workbooks = list(set(to_be_archived_workbooks) - set(active_workbooks))

        return list(set(active_workbooks)), reminder_workbooks, to_be_archived_workbooks

    except Exception as error: 
        logger.error(f"Error while connecting to server repositry. {error}")
        logger.error(error, exc_info=True)

    finally:
        if conn:  
            curr.close()
            conn.close()



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

        to_be_archived = []
        workbook_name = []

        for wb in TSC.Pager(server.workbooks):
            url_id = wb._webpage_url.split("workbooks/")[1]

            if url_id not in active_workbooks: 

                if wb.project_id != dest_project.id:
                    wb.project_id = dest_project.id

                    archive_list.append(wb.id)

                    if wb.name in archive_list: 
                        wb.name = wb.name + "_2"
                        to_be_archived.append(wb.name)
                        workbook_name.apped(wb.name)
                        server.workbooks.update(wb)

                    else: 
                        to_be_archived.append(wb.name)
                        workbook_name.apped(wb.name)
                        server.workbooks.update(wb)

        return to_be_archived, workbook_name 


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


def main(server_url, site_name, username, password, archive_project, api_conf, version): 
    
    ##### STEP 1: Find workbooks that are: active, to be archived and to be reminded. #####
    active_workbooks, reminder_workbooks, to_be_archived_workbooks = workbook_classifier()
    stale_workbooks_ids, stale_workbook_names = archive_workbooks(server_url, site_name, username, password, archive_project, active_workbooks)
    
    ##### STEP 2: Remove extract refresh tasks from archived workbooks #####
    extract_task_list = extract_tasks(api_conf)
    delete_archived_workbook_tasks(server_url, site_name, username, password, archive_project, extract_task_list)

    ##### STEP 2: Remove extract refresh tasks from archived workbooks #####
    archive_notifications(reminder_workbooks, to_be_archived_workbooks)

    ##### STEP 3: Download workbook to sharepoint and delete of server #####
    download_and_delete_wokrbooks(username, password, site_name, server_url, workbook_names, filename)


if __name__ == "__main__":

    main()