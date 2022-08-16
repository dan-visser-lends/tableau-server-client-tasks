import psycopg2
import logging
import pandas as pd
import datetime
from datetime import timedelta 
from dateutil.relativedelta import relativedelta


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
        reminder_limit = now - relativedelta(months=3) - relativedelta(weeks=1)
        delete_limit = now - relativedelta(months=3) - relativedelta(weeks=2)

        active_workbooks = df[(df["last_view_time"]>active_limit)]["views_workbook_id"].astype(str).to_list()
        reminder_workbooks = df[(df["last_view_time"]>reminder_limit)&(df["last_view_time"]<active_limit)]["views_workbook_id"].astype(str).to_list()
        to_be_deleted_workbooks = df[(df["last_view_time"]>delete_limit)&(df["last_view_time"]<reminder_limit)]["views_workbook_id"].astype(str).to_list()

        active_workbooks_owner = df[(df["last_view_time"]>active_limit)]["name"].astype(str).to_list()
        reminder_workbooks_owner = df[(df["last_view_time"]>reminder_limit)&(df["last_view_time"]<active_limit)]["name"].astype(str).to_list()
        to_be_deleted_workbooks_owner = df[(df["last_view_time"]>delete_limit)&(df["last_view_time"]<reminder_limit)]["name"].astype(str).to_list()

        reminder_workbooks = list(set(reminder_workbooks) - set(active_workbooks))
        to_be_deleted_workbooks = list(set(to_be_deleted_workbooks) - set(active_workbooks))
        
        reminder_workbooks = list(set(reminder_workbooks_owner) - set(active_workbooks_owner))
        to_be_deleted_workbooks = list(set(to_be_deleted_workbooks_owner) - set(active_workbooks_owner))

        return list(set(active_workbooks)), reminder_workbooks, to_be_deleted_workbooks, reminder_workbooks_owner, to_be_deleted_workbooks_owner


    except Exception as error: 
        logger.error(f"Error while connecting to server repositry. {error}")
        logger.error(error, exc_info=True)

    finally:
        if conn:  
            curr.close()
            conn.close()


def archivable_workbooks():

    """
    queries API or TSR to get workbooks whove been in the archive for a month"""





