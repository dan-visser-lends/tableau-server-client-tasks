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


def main():
    
    ##### STEP 1: find workbooks that are: active, to be archived, to be reminded, and to be deleted #####
    active_workbooks, reminder_workbooks, to_be_archived_workbooks, to_be_deleted = workbook_classifier()

    ##### STEP 2: send reminders to workbook owners who's workbooks are going to be archive if not accessed #####
    reminders(reminder_workbooks)
    
    ##### STEP 3: Archive workbooks and remove extract refresh #####
    archive_workbooks(active_workbooks, to_be_archived_workbooks)

    ##### STEP 4: download, upload to shrepoint and deleted workbooks that have been archived for over a month #####
    delete_workbooks(to_be_deleted)

if __name__ == '__main__':

    main()