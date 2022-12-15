#!/opt/vertica/oss/python3/bin/python3

import argparse
import csv
import datetime
import logging
import os
from pprint import pprint

import traceback
import sys

import vertica_python


########################### GLOBAL VARS ###########################

ARGS = None

CONNECTION = None
CURSOR = None

FAILURE_LIST = []
SUCCESS_LIST = []

KEY_TO_UPDATE = {
    "actualservicecontainsservicecomponent": ["firstentity_actualservice","secondentity_servicecomponent"],
    "actualserviceusesactualservice": ["firstentity_actualservice", "secondentity_actualservice"],
    "agreemententitytargetset": ["firstentity_agreement","secondentity_targetset"],
    "agreementgrouptargetsets": ["firstentity_agreement","secondentity_grouptargetsets"],
    "agreementregisteredforservice": ["firstentity_agreement","secondentity_actualservice"],
    "articlebaseonchange": ["firstentity_change","secondentity_article"],
    "articlebaseonincident": ["firstentity_incident","secondentity_article"],
    "articlebaseonproblem": ["firstentity_problem","secondentity_article"],
    "articlebaseonrelease": ["firstentity_release","secondentity_article"],
    "articlebaseonrequest": ["firstentity_request", "secondentity_article"],
    "articlecoversassetmodel": ["firstentity_article","secondentity_assetmodel"],
    "articlerelatedbyarticle": ["firstentity_article","secondentity_article"],
    "articlestoreviewers": ["firstentity_article","secondentity_person"],
    "articlestoreviewgroups": ["firstentity_article","secondentity_persongroup"],
    "articletoentitlementrules": ["firstentity_article","secondentity_entitlementrule"],
    "categorytoentitlementrules": ["firstentity_category","secondentity_entitlementrule"],
    "changecausedbychange": ["firstentity_change","secondentity_change"],
    "changecausedbyincident": ["firstentity_incident","secondentity_change"],
    "changecausedbyproblem": ["firstentity_problem","secondentity_change"],
    "changecausedbyrelease": ["firstentity_change","secondentity_release"],
    "changecausedbyrequest": ["firstentity_request","secondentity_change"],
    "companymanagedbyperson": ["firstentity_person","secondentity_company"],
    "companysites": ["firstentity_company","secondentity_location"],
    "covereddevice": ["firstentity_contract","secondentity_device"],
    "coveredinfrastructureperipheral": ["firstentity_contract","secondentity_infrastructureperipheral"],
    "coveredlicense": ["firstentity_contract","secondentity_license"],
    "coveredperson": ["firstentity_contract","secondentity_person"],
    "coveredservice": ["firstentity_contract","secondentity_actualservice"],
    "dependsonproposal": ["firstentity_proposal","secondentity_proposal"],
    "deviceaffectedbychange": ["firstentity_device","secondentity_change"],
    "deviceaffectedbyincident": ["firstentity_device","secondentity_incident"],
    "deviceaffectedbyproblem": ["firstentity_device","secondentity_problem"],
    "deviceaffectedbyrequest": ["firstentity_device","secondentity_request"],
    "deviceusedbyperson": ["firstentity_device","secondentity_person"],
    "entitymodelusedbyactualservice": ["firstentity_entitymodel","secondentity_actualservice"],
    "fixedassetaccountedfordevice": ["firstentity_fixedasset","secondentity_device"],
    "fixedassetaccountedforinfrastructureperipheral": ["firstentity_fixedasset","secondentity_infrastructureperipheral"],
    "fixedassetaccountedforlicense": ["firstentity_fixedasset","secondentity_license"],
    "grouptargetsetsassociatedgroups": ["firstentity_grouptargetsets","secondentity_persongroup"],
    "grouptargetsetsentitytargetset": ["firstentity_grouptargetsets","secondentity_targetset"],
    "grouptoperson": ["firstentity_persongroup","secondentity_person"],
    "idearelatedtoidea": ["firstentity_idea","secondentity_idea"],
    "incidentcausedbychange": ["firstentity_incident","secondentity_change"],
    "incidentcausedbyincident": ["firstentity_incident","secondentity_incident"],
    "incidentcausedbyproblem": ["firstentity_incident","secondentity_problem"],
    "incidentcausedbyrelease": ["firstentity_incident","secondentity_release"],
    "incidentcausedbyrequest": ["firstentity_incident","secondentity_request"],
    "incidentescalationmembersperson": ["firstentity_incident","secondentity_person"],
    "infrastructureperipheralaffectedbychange": ["firstentity_infrastructureperipheral","secondentity_change"],
    "infrastructureperipheralaffectedbyincident": ["firstentity_infrastructureperipheral","secondentity_incident"],
    "infrastructureperipheralaffectedbyproblem": ["firstentity_infrastructureperipheral","secondentity_problem"],
    "infrastructureperipheralaffectedbyrequest": ["firstentity_infrastructureperipheral","secondentity_request"],
    "licenseaffectedbychange": ["firstentity_license","secondentity_change"],
    "licenseaffectedbyincident": ["firstentity_license","secondentity_incident"],
    "licenseaffectedbyproblem": ["firstentity_license","secondentity_problem"],
    "licenseaffectedbyrequest": ["firstentity_license","secondentity_request"],
    "offeringbundlestoofferings": ["firstentity_offering","secondentity_offering"],
    "offeringcoversassetmodel": ["firstentity_offering","secondentity_assetmodel"],
    "offeringtoentitlementrules": ["firstentity_offering","secondentity_entitlementrule"],
    "optimizationcauseproposal": ["firstentity_optimization","secondentity_proposal"],
    "persontogroup": ["firstentity_person","secondentity_persongroup"],
    "portfoliotoservicedefinition": ["firstentity_portfolio","secondentity_servicedefinition"],
    "problemcausedbychange": ["firstentity_change","secondentity_problem"],
    "problemcausedbyrelease": ["firstentity_problem","secondentity_release"],
    "problemcausedbyrequest": ["firstentity_request","secondentity_problem"],
    "problemhasduplicate": ["firstentity_problem","secondentity_problem"],
    "projectcausedbychange": ["firstentity_project","secondentity_change"],
    "projectcausedbyrelease": ["firstentity_project","secondentity_release"],
    "proposalcausechange": ["firstentity_proposal","secondentity_change"],
    "proposalrelatedideas": ["firstentity_proposal","secondentity_idea"],
    "relatedtodevices": ["firstentity_device","secondentity_device"],
    "relatedtoinfrastructureperipherals": ["firstentity_infrastructureperipheral","secondentity_device"],
    "releasecausedbyincident": ["firstentity_release","secondentity_incident"],
    "requestcausedbychange": ["firstentity_change","secondentity_request"],
    "requestcausedbyincident": ["firstentity_incident","secondentity_request"],
    "requestcausedbyproblem": ["firstentity_problem","secondentity_request"],
    "requestcausedbyrequest": ["firstentity_request","secondentity_request"],
    "requestfulfilledbyactualservice": ["firstentity_request","secondentity_actualservice"],
    "requestfulfilledbydevice": ["firstentity_request","secondentity_device"],
    "requestfulfilledbyinfrastructureperipheral": ["firstentity_request","secondentity_infrastructureperipheral"],
    "servicecomponentcontainsdevice": ["firstentity_servicecomponent","secondentity_device"],
    "servicecomponentcontainssystemelement": ["firstentity_servicecomponent","secondentity_systemelement"],
    "servicecomponentusesactualservice": ["firstentity_servicecomponent","secondentity_actualservice"],
    "servicedefinitionsmexperts": ["firstentity_servicedefinition","secondentity_person"],
    "servicedefinitiontoentitlementrules": ["firstentity_servicedefinition","secondentity_entitlementrule"],
    "stockroomservedbylocation": ["firstentity_stockroom","secondentity_location"],
    "systemelementaffectedbychange": ["firstentity_systemelement","secondentity_change"],
    "systemelementaffectedbyincident": ["firstentity_systemelement","secondentity_systemelement"],
    "systemelementaffectedbyproblem": ["firstentity_systemelement","secondentity_problem"],
    "systemelementaffectedbyrequest": ["firstentity_systemelement","secondentity_request"],
    "systemelementcontainsdevice": ["firstentity_systemelement","secondentity_device"],
    "tpdappliestoactualservice": ["firstentity_timeperioddefinition","secondentity_actualservice"],
    "tpdappliestoservicedefinition": ["firstentity_timeperioddefinition","secondentity_servicedefinition"],
    "tpdappliestosystemelement": ["firstentity_timeperioddefinition","secondentity_systemelement"],
    "tpdhasruleexceptions": ["firstentity_timeperioddefinition","secondentity_timeperioddefinition"],
    "worksatlocation": ["firstentity_person","secondentity_location"],
    "accountcode": "id",
    "actualservice": "id",
    "agreement": "id",
    "article": "id",
    "assetmodel": "id",
    "brand": "id",
    "budgetcenter": "id",
    "category": "id",
    "change": "id",
    "company": "id",
    "contract": "id",
    "costcenter": "id",
    "costtype": "id",
    "device": "id",
    # "device_diskdevice": ["id", "parentrowid", "index"],
    # "device_filesystem": ["id", "parentrowid", "index"],
    # "device_ipaddress": ["id", "parentrowid"],
    # "device_networkcard": ["id", "parentrowid", "index"],
    # "device_runningsoftware": ["id", "parentrowid", "index"],
    "entitlementrule": "id",
    "entitymodel": "id",
    "externalsystem": "id",
    "fixedasset": "id",
    "fulfillmentplan": "id",
    "grouptargetsets": "id",
    "holiday": "id",
    "idea": "id",
    "incident": "id",
    "infrastructureperipheral": "id",
    "itprocessrecordcategory": "id",
    "license": "id",
    "location": "id",
    "offering": "id",
    "persongroup": "id",
    "person": "id",
    "portfolio": "id",
    "problem": "id",
    "project": "id",
    "proposal": "id",
    "recordsltstatus": "id",
    "request": "id",
    "servicecomponent": "id",
    "servicedefinition": "id",
    "serviceleveltarget": "id",
    "stockroom": "id",
    "subscription": "id",
    "systemelement": "id",
    "targetdefinition": "id",
    "targetset": "id",
    "task": "id",
    "timeperioddefinition": "id"
}

NEW_ROWS_IDENTIFIER = [
    'ADD_RELATION',
    'ADD_ENTITY',    
]

UPDATE_ROW_IDENTIFIER = [
    'UPDATE_ENTITY',     
]

REMOVE_ROW_IDENTIFER = [
    'REMOVE_ENTITY',
    'REMOVE_RELATION'
]

MASTER_TABLES = [
    'accountcode',
    'actualservice',
    'agreement',
    'article',
    'assetmodel',
    'brand',
    'budgetcenter',
    'category',
    'change',
    'company',
    'contract',
    'costcenter',
    'costtype',
    'device',    
    'entitlementrule',
    'entitymodel',
    'externalsystem',
    'fixedasset',
    'fulfillmentplan',
    'grouptargetsets',
    'holiday',
    'idea',
    'incident',
    'infrastructureperipheral',
    'itprocessrecordcategory',
    'license',
    'location',
    'offering',
    'persongroup',
    'person',
    'portfolio',
    'problem',
    'project',
    'proposal',
    'recordsltstatus',
    'request',
    'servicecomponent',
    'servicedefinition',
    'stockroom',
    'subscription',
    'systemelement',
    'targetdefinition',
    'targetset',
    'task',
    'timeperioddefinition',
    'serviceleveltarget',
]

RELATIONSHIP_TABLES = [
    'actualservicecontainsservicecomponent',
    'actualserviceusesactualservice',
    'agreemententitytargetset',
    'agreementgrouptargetsets',
    'agreementregisteredforservice',
    'articlebaseonchange',
    'articlebaseonincident',
    'articlebaseonproblem',
    'articlebaseonrelease',
    'articlebaseonrequest',
    'articlecoversassetmodel',
    'articlerelatedbyarticle',
    'articlestoreviewers',
    'articlestoreviewgroups',
    'articletoentitlementrules',
    'categorytoentitlementrules',
    'changecausedbychange',
    'changecausedbyincident',
    'changecausedbyproblem',
    'changecausedbyrelease',
    'changecausedbyrequest',
    'companymanagedbyperson',
    'companysites',
    'covereddevice',
    'coveredinfrastructureperipheral',
    'coveredlicense',
    'coveredperson',
    'coveredservice',
    'dependsonproposal',
    'deviceaffectedbychange',
    'deviceaffectedbyincident',
    'deviceaffectedbyproblem',
    'deviceaffectedbyrequest',
    'deviceusedbyperson',    
    'entitymodelusedbyactualservice',
    'fixedassetaccountedfordevice',
    'fixedassetaccountedforinfrastructureperipheral',
    'fixedassetaccountedforlicense',
    'grouptargetsetsassociatedgroups',
    'grouptargetsetsentitytargetset',
    'grouptoperson',
    'idearelatedtoidea',
    'incidentcausedbychange',
    'incidentcausedbyincident',
    'incidentcausedbyproblem',
    'incidentcausedbyrelease',
    'incidentcausedbyrequest',
    'incidentescalationmembersperson',
    'infrastructureperipheralaffectedbychange',
    'infrastructureperipheralaffectedbyincident',
    'infrastructureperipheralaffectedbyproblem',
    'infrastructureperipheralaffectedbyrequest',
    'licenseaffectedbychange',
    'licenseaffectedbyincident',
    'licenseaffectedbyproblem',
    'licenseaffectedbyrequest',
    'offeringbundlestoofferings',
    'offeringcoversassetmodel',
    'offeringtoentitlementrules',
    'optimizationcauseproposal',
    'persontogroup',
    'portfoliotoservicedefinition',
    'problemcausedbychange',
    'problemcausedbyrelease',
    'problemcausedbyrequest',
    'problemhasduplicate',
    'projectcausedbychange',
    'projectcausedbyrelease',
    'proposalcausechange',
    'proposalrelatedideas',
    'relatedtodevices',
    'relatedtoinfrastructureperipherals',
    'releasecausedbyincident',
    'requestcausedbychange',
    'requestcausedbyincident',
    'requestcausedbyproblem',
    'requestcausedbyrequest',
    'requestfulfilledbyactualservice',
    'requestfulfilledbydevice',
    'requestfulfilledbyinfrastructureperipheral',
    'servicecomponentcontainsdevice',
    'servicecomponentcontainssystemelement',
    'servicecomponentusesactualservice',
    'servicedefinitionsmexperts',
    'servicedefinitiontoentitlementrules',
    'stockroomservedbylocation',
    'systemelementaffectedbychange',
    'systemelementaffectedbyincident',
    'systemelementaffectedbyproblem',
    'systemelementaffectedbyrequest',
    'systemelementcontainsdevice',
    'tpdappliestoactualservice',
    'tpdappliestoservicedefinition',
    'tpdappliestosystemelement',
    'tpdhasruleexceptions',
    'worksatlocation',
]

TRANSACTION_TABLES = [
    'device_cpu',
    'device_diskdevice',
    'device_filesystem',
    'device_ipaddress',
    'device_networkcard',
    'device_runningsoftware',
    'comments',
]

# COMMENTS_TABLES = [
#     'actualservice_comments',
#     'article_comments',
#     'assetmodel_comments',
#     'brand_comments',
#     'category_comments',
#     'change_comments',
#     'company_comments',
#     'contract_comments',
#     'costcenter_comments',
#     'costtype_comments',    
#     'device_comments',
#     'entitlementrule_comments',
#     'externalsystem_comments',
#     'fixedasset_comments',
#     'fulfillmentplan_comments',
#     'group_comments',
#     'holiday_comments',
#     'idea_comments',
#     'incident_commets',
#     'infrastructureperipheral_comments',
#     'license_comments',
#     'location_comments',
#     'offering_comments',
#     'person_comments',
#     'portfolio_comments',
#     'problem_comments',
#     'project_comments',
#     'proposal_comments',
#     'request_comments',
#     'servicecomponent_comments',
#     'servicedefinition_comments',
#     'stockroom_comments',
#     'systemelement_comments',
#     'targetdefinition_comments',
#     'targetset_comments',
#     'task_comments',
#     'timeperioddefinition_comments',
# ]

# COMMENTS_TABLES = [
#     'comments',
# ]

###################################################################


def get_args():
    """ Process and Return command line arguments """
    parser = argparse.ArgumentParser(description='Process CSV files and insert to DV')
    parser.add_argument('csv_files_dir', help='Folder Path of CSV Files')
    parser.add_argument('schema', help='schema name')    
    parser.add_argument('db', help='Database name')
    parser.add_argument('db_user', help='DB username')
    parser.add_argument('db_pass', help='DB password')
    parser.add_argument('sync_type', help='Sync Type')
    return parser.parse_args()


def get_csv_files():
    """
    Returns CSV files in given folder
    """
    try:
        # list files in dir
        files = os.listdir(ARGS.csv_files_dir)
    except FileNotFoundError:
        logging.error(f'No such directory: {ARGS.csv_files_dir}')
        exit(1)
    except Exception as e:
        logging.error(f'An error occurred while listing files from dir: {ARGS.csv_files_dir}')
        logging.error(e)
        logging.error(traceback.format_exc())
        exit(1)
        
    # filter CSV files
    files = list(filter(lambda f: f.endswith('.csv'), files))

    # sort csv files a-z
    files.sort()    

    return files


def get_db_connection():
    conn_info = {
        'host': '127.0.0.1',
        'port': 5433,
        'user': ARGS.db_user,
        'password': ARGS.db_pass,
        'database': ARGS.db,
        # autogenerated session label by default,
        # 'session_label': 'some_label',
        # default throw error on invalid UTF-8 results
        'unicode_error': 'strict',
        # SSL is disabled by default
        'ssl': False,
        # using server-side prepared statements is disabled by default
        'use_prepared_statements': False,
        # connection timeout is not enabled by default
        # 5 seconds timeout for a socket operation (Establishing a TCP connection or read/write operation)
        'connection_timeout': 300,
        'backup_server_node': ['15.120.115.182', '15.120.115.183']
    }

    connection  = vertica_python.connect(**conn_info)
    return connection


def create_temp_csv(rows, cols, filename):
    """
    create temp csv file without BI Sync header
    """

    # rename filename
    filename = filename.replace('.csv', '_new.csv')
    file_    = os.path.join('/tmp', filename)

    # headers  = list(rows[0].keys())

    with open(file_, 'w', encoding='utf-8', newline='') as fd:
        writer = csv.DictWriter(fd, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    return file_


def execute_sql_command(cmd, filename):
    """
    Exceute DB Command
    """

    try:
        CURSOR.execute(cmd)
        logging.info(f'Result: {CURSOR.fetchone()[0]}')
        CURSOR.execute('commit')
        return True
    except Exception as e:
        logging.error(f'An error occurred while inserting records from csv {filename}')
        logging.error(traceback.format_exc())        
        FAILURE_LIST.append(filename)
        # exit(1)


def delete_trans_records(trans_table, filename):
    """
    delete records from transaction table
    """
    logging.info(f'deleting records from table {trans_table}')
    cmd = f'delete from {ARGS.schema}.{trans_table}'
    return execute_sql_command(cmd, filename)


def delete_records(table, filename, rows):
    """
    delete records from transaction table
    """

    # check for records
    if len(rows) > 0:
        logging.info(f'deleting records from table {table}')
        # columns = list(map(lambda x: x.lower(), rows[0].keys()))

        unique_id = KEY_TO_UPDATE[table]

        if type(unique_id) == list:            

            condition = []
                
            for row in rows:
                row_lower = {k.lower():v for k, v in row.items()}
                temp = []

                for rec_id in unique_id:
                    temp.append(f"{rec_id}='{row_lower[rec_id]}'")

                condition.append(f'({" AND ".join(temp)})')
                
            where_cond = ' OR '.join(condition)

            cmd = f'DELETE FROM {ARGS.schema}.{table} WHERE {where_cond}'            

        else:                    
            values = []

            # iterate each row to get values
            for row in rows:
                row_lower = {k.lower():v for k, v in row.items()}
                values.append(f"'{row_lower[unique_id]}'")

            unique_ids = ','.join(values)

            cmd = f'DELETE FROM {ARGS.schema}.{table} WHERE {unique_id} IN ({unique_ids})'        
        
        return execute_sql_command(cmd, filename)


def insert_records(table, columns, csv_file, filename):
    """
    insert records to table
    """
    logging.info(f'inserting records to table {table}')

    # construct COPY command to insert data
    columns = ','.join(columns)
    cmd = f"COPY {ARGS.schema}.{table}({columns}) FROM '{csv_file}' PARSER fcsvparser(type='traditional', delimiter=',')"
    return execute_sql_command(cmd, filename)


def merge_data(table, trans_table, filename, columns):
    """
    Executes merge command
    """
    logging.info(f'merging table data: {trans_table} -> {table}')

    # condtruct matching condtion
    unique_id = KEY_TO_UPDATE[table]

    if type(unique_id) == str:
        matching_cond = f'SRC.{unique_id}=TGT.{unique_id}'
    elif type(unique_id) == list:
        matching_cond = ' AND '.join(f'SRC.{col}=TGT.{col}' for col in unique_id)

    # construct cols to update & insert
    cols_to_update = ','.join(f'{col}=SRC.{col}' for col in columns)
    cols_to_insert = ','.join(columns)

    # construct insertion command
    insert_data = 'SRC.' + ',SRC.'.join(columns)

    # construct merge command
    cmd = f"MERGE INTO {ARGS.schema}.{table} TGT USING {ARGS.schema}.{trans_table} SRC ON {matching_cond} WHEN MATCHED THEN UPDATE SET {cols_to_update} WHEN NOT MATCHED THEN INSERT ({cols_to_insert}) VALUES ({insert_data})"
    return execute_sql_command(cmd, filename)


def identify_record_type(rows):
    """
    Identify the records type
    """
    new_and_update_rows = []
    # update_rows = []
    delete_rows = []

    for row in rows:
        if 'BiSyncOperation' in row:
            if row['BiSyncOperation'] in NEW_ROWS_IDENTIFIER:
                new_and_update_rows.append(row)
            elif row['BiSyncOperation'] in UPDATE_ROW_IDENTIFIER:
                new_and_update_rows.append(row)
            elif row['BiSyncOperation'] in REMOVE_ROW_IDENTIFER:
                delete_rows.append(row)
        else:
            new_and_update_rows.append(row)
    return new_and_update_rows, delete_rows


def read_csv(file_):
    """ Reads CSV data and convert to dict"""

    with open(file_, encoding='utf-8') as fd:
        csvreader = csv.DictReader(fd)
        headers = [col.lower() for col in csvreader.fieldnames]
        rows = [row for row in csvreader]

    return rows


def remove_bisync_cols(row):
    if 'RowId' in row:
        row.pop('RowId')
    if 'BiSyncOperation' in row:
        row.pop('BiSyncOperation')
    return row


def get_records_count(table):
    """
    Executes merge command
    """
    
    # construct sql query
    qry = f"SELECT COUNT(*) FROM {ARGS.schema}.{table}"
    
    try:
        CURSOR.execute(qry)
        return CURSOR.fetchone()[0]        
    except Exception as e:
        logging.error(f'An error occurred while executing query: {qry}')
        logging.error(traceback.format_exc())
        FAILURE_LIST.append(filename)        



def sync_data(csv_file, table):
    """
    Sync CSV file data to vertica table
    """

    logging.info(f'{"#" * 15}  {table}  {"#" * 15}')
    logging.info(f'Reading CSV: {csv_file}')

    # read csv data
    rows = read_csv(csv_file)    
    logging.info(f'{len(rows)} record(s) found')

    # extract filename
    filename = os.path.basename(csv_file)


    if len(rows) > 0:

        # get records count before sync
        records_count_before_sync = get_records_count(table)
        # rows = verify_columns(rows, table, filename)        

        # identify the record types
        new_and_update_rows, delete_rows = identify_record_type(rows)
        # print(new_and_update_rows)
        
        logging.info(f'{len(new_and_update_rows)} new & updated entries found')        
        logging.info(f'{len(delete_rows)} removable entries found')

        if table not in TRANSACTION_TABLES:
            # remove BI sync header
            logging.info(f'Removing BI Sync Headers')
            # rows = list(map(remove_bisync_cols, rows))
            new_and_update_rows = list(map(remove_bisync_cols, new_and_update_rows))            
            delete_rows = list(map(remove_bisync_cols, delete_rows))
        
        # delete rows        
        delete_records(table, filename, delete_rows)        
        

        # check for new and updated records
        if len(new_and_update_rows) > 0:

            # verify the columns
            new_and_update_rows = verify_columns(new_and_update_rows, table, filename)

            columns = list(new_and_update_rows[0].keys())
            columns_lower = [col.lower() for col in columns]

            # create temp csv file
            temp_csv_file = create_temp_csv(new_and_update_rows, columns, filename)

            # check table type
            if table in MASTER_TABLES:
                # get transaction table name
                trans_table = f'{table}__transaction' 

                # delete if any existing records from transaction table
                if not delete_trans_records(trans_table, filename):
                    return

                # insert record to transaction table
                if not insert_records(trans_table, columns_lower, temp_csv_file, filename):
                    return

                # merge tables
                if not merge_data(table, trans_table, filename, columns_lower):
                    return

                SUCCESS_LIST.append(filename)

            elif table in RELATIONSHIP_TABLES or table in TRANSACTION_TABLES:
                if not insert_records(table, columns, temp_csv_file, filename):
                    return
                SUCCESS_LIST.append(filename)

            logging.info(f'Number of records in table before sync: {records_count_before_sync}')
            logging.info(f'Number of records in table after sync:  {get_records_count(table)}')


            logging.info(f'Deleting temp csv_file: {temp_csv_file}')
            os.remove(temp_csv_file)
    else:        
        SUCCESS_LIST.append(filename)


def initial_sync(csv_file, table):
    """
    Performs initial sync
    """    

    # logging.info(f'{"#" * 15}  {table}  {"#" * 15}')
    logging.info(f'Identified Table {table}')
    logging.info(f'Reading CSV: {csv_file}')

    # read csv data
    rows = read_csv(csv_file)    
    logging.info(f'{len(rows)} record(s) found')

    if len(rows) > 0:

        # extract filename
        filename = os.path.basename(csv_file)
        if table not in TRANSACTION_TABLES:
            logging.info(f'Removing BI Sync Headers')
            rows = list(map(remove_bisync_cols, rows))

        rows = verify_columns(rows, table, filename)

        columns = list(rows[0].keys())
        columns_lower = [col.lower() for col in columns]

        # create temp csv file
        temp_csv_file = create_temp_csv(rows, columns, filename)

        # insert rows
        if insert_records(table, columns_lower, temp_csv_file, filename):
            SUCCESS_LIST.append(filename)

        logging.info(f'Deleting temp csv_file: {temp_csv_file}')
        os.remove(temp_csv_file)

    else:
        SUCCESS_LIST.append(filename)       


def verify_columns(rows, table, filename):
    """verify the csv columns against table data"""
    cols = get_table_cols(table, filename)
    cols = [col.lower() for col in cols]

    keys = [key.lower() for key in rows[0].keys()]

    logging.info('Verifing columns present in csv file')
    logging.info(f'{len(keys)} columns found in csv and {len(cols)} found in table')    

    if len(cols) == len(keys):
        return rows
    else:
        diff_cols = list(set(cols) - set(keys))
        for row in rows: 
            for col in diff_cols:
                row.update({col: ''})
        
        return rows



def get_table_cols(table, filename):
    """ return list of columns of given table"""
    qry = f"SELECT column_name FROM columns WHERE table_schema = '{ARGS.schema}' AND table_name = '{table}'"

    try:
        CURSOR.execute(qry)
        results = CURSOR.fetchall()
        cols = [row[0] for row in results]        
        # logging.info(f'Result: {cols}')
        return cols
    except Exception as e:
        logging.error(f'An error occurred while executing query: {qry}')
        logging.error(traceback.format_exc())
        FAILURE_LIST.append(filename)
        # exit(1)


def main():
    """
    Main function
    """

    global ARGS, CURSOR, CONNECTION, SUCCESS_LIST, FAILURE_LIST

    # configure log params
    logging.basicConfig(
        # filename='/home/dbadmin/logs/db_sync.log',
        # filemode='a',
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )

    # get command line args
    ARGS = get_args()

    # print(ARGS)

    # set csv file size to sys max
    csv.field_size_limit(sys.maxsize)

    # get csv files
    files = get_csv_files()

    logging.info(f'Total {len(files)} csv files found in dir {ARGS.csv_files_dir}')

    # if no csv files found
    if len(files) == 0:
        exit(0)

    logging.info('Initiating DB connection')
    CONNECTION  = get_db_connection()
    CURSOR      = CONNECTION.cursor()

    all_tables = MASTER_TABLES + TRANSACTION_TABLES + RELATIONSHIP_TABLES

    for file_ in files:        
        logging.info(f'Processing File: {file_}')

        # find table
        _last_index = file_.rfind('_')
        table = file_[:_last_index].lower()

        if 'comment' in table:
            table = 'comments'        
        
        # verify table

        if table is None or table not in all_tables:
            if table is None:
                error = f'unable to find table name from csv file {file_}'
            elif table not in all_tables:
                error = f'Invalid Table {table}, verify the csv file {file_}'

            logging.error(error)
            FAILURE_LIST.append(file_)            

            # continue to next file
            continue

        csv_file = os.path.join(ARGS.csv_files_dir, file_)

        # check sync type
        if ARGS.sync_type.lower() == 'initial' or table == 'comments':
            initial_sync(csv_file, table)
        else:            
            sync_data(csv_file, table)        

    logging.info('Closing DB connection')
    CONNECTION.close()

    logging.info('------SUMMARY-------')    

    logging.info('-----Successfull START----')
    logging.info(f'Successfull CSV list: {len(SUCCESS_LIST)}')

    if len(SUCCESS_LIST) > 0:                
        logging.info('\n'.join(SUCCESS_LIST))

    logging.info('-----Successfull END----')

    logging.info('-----Failure START----')
    logging.info(f'Failure CSV list: {len(FAILURE_LIST)}')
    
    if len(FAILURE_LIST) > 0:                
        logging.info('\n'.join(list(FAILURE_LIST)))
        exit(1)

if __name__ == '__main__':
    main()
