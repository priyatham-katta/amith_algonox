import argparse
import ast
import json
import requests
import traceback
import warnings

from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from pandas import Series, Timedelta, to_timedelta
from time import time

from db_utils import DB
from get_fields_info import get_fields_info
from get_fields_info_utils import sort_ocr

from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span

def http_transport(encoded_span):
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body =encoded_span
    requests.post(
            'http://service_bridge:5002/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )


app = Flask(__name__)
CORS(app)

warnings.filterwarnings('ignore')

def isListEmpty(inList):
    if isinstance(inList, list):    # Is a list
        return all( map(isListEmpty, inList) )
    return False # Not a list


def update_queue_trace(queue_db,case_id,latest):
    queue_trace_q = "SELECT * FROM `trace_info` WHERE `case_id`=%s"
    queue_trace_df = queue_db.execute(queue_trace_q,params=[case_id])

    if queue_trace_df.empty:
        message = f' - No such case ID `{case_id}` in `trace_info`.'
        print(f'ERROR: {message}')
        return {'flag':False,'message':message}
    # Updating Queue Name trace
    try:
        queue_trace = list(queue_trace_df.queue_trace)[0]
    except:
        queue_trace = ''
    if queue_trace:
        queue_trace += ','+latest
    else:
        queue_trace = latest

    #Updating last_updated_time&date

    try:
        last_updated_dates = list(queue_trace_df.last_updated_dates)[0]
    except:
        last_updated_dates = ''
    if last_updated_dates:
        last_updated_dates += ','+ datetime.now().strftime(r'%d/%m/%Y %H:%M:%S')
    else:
        last_updated_dates = datetime.now().strftime(r'%d/%m/%Y %H:%M:%S')

    update = {'queue_trace':queue_trace}
    where = {'case_id':case_id}
    update_q = "UPDATE `trace_info` SET `queue_trace`=%s, `last_updated_dates`=%s WHERE `case_id`=%s"
    queue_db.execute(update_q,params=[queue_trace,last_updated_dates,case_id])

    return {'flag':True,'message':'Updated Queue Trace'}

@app.route('/get_template_exceptions', methods=['POST', 'GET'])
def get_template_exceptions():
    data = request.json

    start_point = data['start']
    end_point = data['end']
    offset = end_point - start_point

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)

    template_config = {
        'host': 'template_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    template_db = DB('template_db', **template_config)
    # db = DB('queues')

    # TODO: Value of "columns" will come from a database.
    # Columns to display is configured by the user from another screen.
    columns = [
            'case_id',
            'file_name',
            'source_of_invoice',
            'cluster',
            'created_date',
            'batch_id',
            'operator',
        ]
    all_st = time()

    process_queue_df = db.execute("SELECT * from `process_queue` where `queue`= 'Template Exceptions' LIMIT %s, %s", params=[start_point, offset])
    total_files = list(db.execute("SELECT id,COUNT(DISTINCT `case_id`) FROM `process_queue` WHERE `queue`= %s", params=['Template Exceptions'])['COUNT(DISTINCT `case_id`)'])[0]

    print('Loading process queue',time()-all_st)
    rest_st = time()
    # latest_case_file = db.get_latest(process_queue_df, 'case_id', 'created_date')
    # latest_case_file = process_queue_df
    trained_info = template_db.get_all('trained_info')
    # print(latest_case_file)
    try:
        # queue_files = latest_case_file.loc[latest_case_file['queue'] == 'Template Exceptions']
        queue_files = process_queue_df
        files = queue_files[columns].to_dict(orient='records')
        for document in files:
            document['created_date'] = (document['created_date']).strftime(r'%B %d, %Y %I:%M %p')
        trained_templates = sorted(list(trained_info.template_name),key=str.lower)

        # columns.append('force_template')
        print(time()-rest_st)

        if end_point > total_files:
            end_point = total_files

        pagination = {"start": start_point + 1, "end": end_point, "total": total_files}

        return jsonify({'flag': True, 'data': {'columns': columns, 'files': files, 'template_dropdown': trained_templates, 'pagination': pagination}})
    except Exception as e:
        traceback.print_exc()
        message = f'Error occured while getting template exception details. {e}'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

def fetch_qname(db):
    queue_definition = db.get_all('queue_definition')
    try:
        queue_df = queue_definition.loc[queue_id]
    except KeyError as e:
        traceback.print_exc()
        message = f'Some column ID not found in column definition table. {e}'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})
    queue_name = queue_df['name']
    queue_type = queue_df['type']

    return queue_definition, queue_name, queue_type


def columns_in_q(queue_type, start_point, end_point):
    if queue_type == 'train':
        host = 'localhost'
        port = request.host.split(':')[-1]
        route = 'get_template_exceptions'
        print(f' > Redirecting to `get_template_exception` route.')
        response =  requests.post(f'http://{host}:{port}/{route}', json={'start': start_point, 'end': end_point})

        return jsonify(response.json())

    query = "SELECT id, column_id from queue_column_mapping where queue_id = %s"
    queue_column_ids = list(db.execute(query, params=[queue_id]).column_id)

    # Get columns using column ID and above result from column configuration table
    columns_time = time()
    columns_definition = db.get_all('column_definition')
    try:
        columns_df = columns_definition.ix[queue_column_ids]
    except KeyError as e:
        traceback.print_exc()
        message = f'Some column ID not found in column definition table. {e}'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})
    columns = list(columns_df['column_name'])
    return columns


def load_process_q(db):
    invoice_files_df = db.execute("SELECT * from `process_queue` where `queue`= %s LIMIT %s, %s", params=[queue_name, start_point, offset])
    total_files = list(db.execute("SELECT id,COUNT(DISTINCT `case_id`) FROM `process_queue` WHERE `queue`= %s", params=[queue_name])['COUNT(DISTINCT `case_id`)'])[0]
    #invoice_files_df = db.get_all('process_queue',discard=['ocr_data','ocr_text','xml_data'])

    return invoice_files_df, total_files

def button_functions(db,invoice_files_df,columns):
    queue_files = invoice_files_df
    #queue_files = lates_invoice_files_df.loc[lates_invoice_files_df['queue'] == queue_name]
    # print(queue_files)
    files = queue_files[columns].to_dict(orient='records')

    for document in files:
        document['created_date'] = (document['created_date']).strftime(r'%B %d, %Y %I:%M %p')
        try:
            query_result = extraction_db.execute("SELECT `id`,`Verify Operator` from `business_rule` where `case_id`=  %s", params=[document['case_id']])
            document['last_updated_by'] = list(query_result['Verify Operator'])[0]
            query_result = extraction_db.execute("SELECT `id`,`Vendor Name` from `ocr` where `case_id`=  %s", params=[document['case_id']])
            document['vendor_name'] = list(query_result['Vendor Name'])[0].replace('suspicious', '')
        except:
            pass
    # * BUTTONS
    # Get workflow definition for the selected queue ID
    workflow_definition = db.get_all('workflow_definition')
    queue_workflow = workflow_definition.loc[workflow_definition['queue_id'] == queue_id]

    # Get button IDs for the queue ID
    button_ids = list(queue_workflow['button_id'])
    rule_groups = list(queue_workflow['rule_group'])

    # Get buttons' attributes from button definition
    button_definition = db.get_all('button_definition')
    buttons_df = button_definition.ix[button_ids]
    button_attributes = buttons_df.to_dict(orient='records')

    # Add which queue to move to in button attributes
    raw_move_to_ids = [int(id) if id is not None else None for id in list(queue_workflow['move_to']) ]
    move_to_df = queue_definition.loc[raw_move_to_ids, :]
    raw_move_to = list(move_to_df['name'])
    move_to = [None if str(id) == 'nan' else id for id in raw_move_to]
    for index, button in enumerate(button_attributes):
        if rule_groups[index] is not None:
            button['stage'] = rule_groups[index].split(',')
        if move_to[index] is not None:
            button['move_to'] = move_to[index]


    # Get button functions
    button_time = time()
    button_functions_df = db.get_all('button_functions')
    button_function_mapping = db.get_all('button_function_mapping')
    button_id_function_mapping = button_function_mapping.loc[button_function_mapping['button_id'].isin(button_ids)]
    # TODO: Convert this loop into a function. Using it later again for tab_id
    for index, row in button_id_function_mapping.iterrows():
        button_id = row['button_id']
        button_name = button_definition.loc[button_id]['text']
        button_id_function_mapping.loc[index, 'button_id'] = button_name

    for button in button_attributes:
        button_name = button['text']
        button_function_id_df = button_id_function_mapping.loc[button_id_function_mapping['button_id'] == button_name]
        button_function_id = list(button_function_id_df['function_id'])
        button['functions'] = []
        # Add all functions
        for function_id in button_function_id:
            function_id_df = button_functions_df.loc[function_id]
            function = function_id_df.to_dict()
            function['parameters'] = function['parameters'].split(',') # Send list of parameters instead of string
            button['functions'].append(function)

    buttons = list(buttons_df['text'])

    return buttons

def fetch_field_ids(db):
    query = "SELECT field_id from queue_field_mapping where queue_id = %s"
    query = f"SELECT * from field_definition where id in ({query})"
    fields_df = db.execute(query, params=[queue_id])

    return fields_df

def datasrc_stuff(extraction_db, fields_df):
    excel_display_data = {}
    tab_type_mapping = {}
    for index, row in fields_df.iterrows():
        tab_id = row['tab_id']
        tab_name = tab_definition.loc[tab_id]['text']
        tab_source = tab_definition.loc[tab_id]['source']
        tab_type = tab_definition.loc[tab_id]['type']
        fields_df.loc[index, 'tab_id'] = tab_name

        tab_type_mapping[tab_name] = tab_type

        if tab_type == 'excel':
            source_table_name = tab_source + '_source'

            # Get excel source data and convert it to dictionary
            excel_source_data = extraction_db.get_all(source_table_name)

            if tab_name not in excel_display_data:
                excel_display_data[tab_name] = {
                    'column': list(excel_source_data),
                    'data': excel_source_data.to_dict(orient='records')[:100]
                }

    return source_table_name, excel_display_data

def fetch_data(user_db,fields_df,columns, total_files):
    field_attributes = fields_df.to_dict(orient='records')
    tabs = list(fields_df.tab_id.unique())
    # print(f'INFO: - Fields: {fields}')

    if 'cluster' not in columns:
        columns.append('last_updated_by')
        columns.append('vendor_name')

    if end_point > total_files:
        end_point = total_files

    pagination = {"start": start_point + 1, "end": end_point, "total": total_files}

    query = "SELECT id, username from users where role = 'TL_Prod'"
    retrain_access = operator in list(user_db.execute(query).username)

    if queue_name == 'Verify' and retrain_access:
        retrain = 1
    else:
        retrain = 0

    data = {
        'columns': columns,
        'files': files,
        'buttons': button_attributes,
        'field': field_attributes,
        'tabs': tabs,
        'excel_source_data': excel_display_data,
        'tab_type_mapping': tab_type_mapping,
        'pagination': pagination,
        'retrain': retrain
    }

    return data

@app.route('/get_queue', methods=['POST', 'GET'])
@app.route('/get_queue/<queue_id>', methods=['POST', 'GET'])
def get_queue(queue_id=None):
    rt_time = time()
    data = request.json
    operator = data.pop('user', None)

    print(operator, "Operatorrrrrrrrrrrrrrrrrr")

    try:
        start_point = data['start'] - 1
        end_point = data['end']
        offset = end_point - start_point
    except:
        start_point = 0
        end_point = 50
        offset = 50

    if queue_id is None:
        message = f'Queue ID not provided.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    try:
        queue_id = int(queue_id)
        # print(f'Queue ID: {queue_id}')
    except ValueError:
        traceback.print_exc()
        message = f'Invalid queue. Expected queue ID to be integer. Got {queue_id}.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    # db = DB('queues', **db_config)
    # db = DB('queues')
    # inti_st = time()
    db = DB('queues', **db_config)
    # db = DB('queues')
    # print('time taken for initialising db',time()-inti_st)

    user_db_config = {
        'host': 'login_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }

    user_db = DB('authentication', **user_db_config)

    extraction_db_config = {
        'host': 'extraction_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    extraction_db = DB('extraction', **extraction_db_config)

    if operator is not None:
        update_ocr(queue_db, operator, case_id)
        # Update the time spent on the particular file
        # update = {
        #     'operator': None,
        # }
        # where = {
        #     'operator': operator
        # }
        # oper_st = time()
        # # db.update('process_queue', update=update, where=where)
        # print('updating')
        # update_operator_q = "UPDATE `process_queue` SET `operator`=%s WHERE `operator`=%s"
        # db.execute(update_operator_q,params=[None,operator])
        # print('updated')
        # # print('Time taken for operator update',time()-oper_st)
    # Get queue name using queue ID
    qid_st = time()
    # queue_definition = db.get_all('queue_definition')
    # try:
    #     queue_df = queue_definition.loc[queue_id]
    # except KeyError as e:
    #     traceback.print_exc()
    #     message = f'Some column ID not found in column definition table. {e}'
    #     print(f'ERROR: {message}')
    #     return jsonify({'flag': False, 'message': message})
    # queue_name = queue_df['name']
    # queue_type = queue_df['type']
    queue_definition, queue_name, queue_type = fetch_qname(db)
    print(f'INFO: - Queue name: {queue_name}')
    # print('Time taken for fetching q name',time()-qid_st)

    # if queue_type == 'train':
    #     host = 'localhost'
    #     port = request.host.split(':')[-1]
    #     route = 'get_template_exceptions'
    #     print(f' > Redirecting to `get_template_exception` route.')
    #     response =  requests.post(f'http://{host}:{port}/{route}', json={'start': start_point, 'end': end_point})

    #     return jsonify(response.json())

    # # * COLUMNS
    # # Get column IDs for requested queue ID from queue-column mappings data
    # # queue_column_mapping = db.get_all('queue_column_mapping')
    # # queue_column_id_df = queue_column_mapping.loc[queue_column_mapping['queue_id'] == queue_id]
    # # queue_column_ids = list(queue_column_id_df['column_id'])

    # query = "SELECT id, column_id from queue_column_mapping where queue_id = %s"
    # queue_column_ids = list(db.execute(query, params=[queue_id]).column_id)

    # # Get columns using column ID and above result from column configuration table
    # columns_time = time()
    # columns_definition = db.get_all('column_definition')
    # try:
    #     columns_df = columns_definition.ix[queue_column_ids]
    # except KeyError as e:
    #     traceback.print_exc()
    #     message = f'Some column ID not found in column definition table. {e}'
    #     print(f'ERROR: {message}')
    #     return jsonify({'flag': False, 'message': message})
    # columns = list(columns_df['column_name'])
    # # print(f'INFO: - Columns to select: {columns}')
    # print('Time taken for columns in q',time()-columns_time)
    columns = columns_in_q(queue_type, start_point, end_point)
    # Get data related to the queue name from OCR table
    # all_st = time()
    # invoice_files_df = db.execute("SELECT * from `process_queue` where `queue`= %s LIMIT %s, %s", params=[queue_name, start_point, offset])
    # total_files = list(db.execute("SELECT id,COUNT(DISTINCT `case_id`) FROM `process_queue` WHERE `queue`= %s", params=[queue_name])['COUNT(DISTINCT `case_id`)'])[0]
    # #invoice_files_df = db.get_all('process_queue',discard=['ocr_data','ocr_text','xml_data'])
    # print('Loading process queue',time()-all_st)
    invoice_files_df, total_files = load_process_q(db)
    #lates_invoice_files_df = db.get_latest(invoice_files_df, 'case_id', 'created_date')

    # queue_files = invoice_files_df
    # #queue_files = lates_invoice_files_df.loc[lates_invoice_files_df['queue'] == queue_name]
    # # print(queue_files)
    # files = queue_files[columns].to_dict(orient='records')

    # for document in files:
    #     document['created_date'] = (document['created_date']).strftime(r'%B %d, %Y %I:%M %p')
    #     try:
    #         query_result = extraction_db.execute("SELECT `id`,`Verify Operator` from `business_rule` where `case_id`=  %s", params=[document['case_id']])
    #         document['last_updated_by'] = list(query_result['Verify Operator'])[0]
    #         query_result = extraction_db.execute("SELECT `id`,`Vendor Name` from `ocr` where `case_id`=  %s", params=[document['case_id']])
    #         document['vendor_name'] = list(query_result['Vendor Name'])[0].replace('suspicious', '')
    #     except:
    #         pass
    # # * BUTTONS
    # # Get workflow definition for the selected queue ID
    # workflow_definition = db.get_all('workflow_definition')
    # queue_workflow = workflow_definition.loc[workflow_definition['queue_id'] == queue_id]

    # # Get button IDs for the queue ID
    # button_ids = list(queue_workflow['button_id'])
    # rule_groups = list(queue_workflow['rule_group'])

    # # Get buttons' attributes from button definition
    # button_definition = db.get_all('button_definition')
    # buttons_df = button_definition.ix[button_ids]
    # button_attributes = buttons_df.to_dict(orient='records')

    # # Add which queue to move to in button attributes
    # raw_move_to_ids = [int(id) if id is not None else None for id in list(queue_workflow['move_to']) ]
    # move_to_df = queue_definition.loc[raw_move_to_ids, :]
    # raw_move_to = list(move_to_df['name'])
    # move_to = [None if str(id) == 'nan' else id for id in raw_move_to]
    # for index, button in enumerate(button_attributes):
    #     if rule_groups[index] is not None:
    #         button['stage'] = rule_groups[index].split(',')
    #     if move_to[index] is not None:
    #         button['move_to'] = move_to[index]


    # # Get button functions
    # button_time = time()
    # button_functions_df = db.get_all('button_functions')
    # button_function_mapping = db.get_all('button_function_mapping')
    # button_id_function_mapping = button_function_mapping.loc[button_function_mapping['button_id'].isin(button_ids)]
    # # TODO: Convert this loop into a function. Using it later again for tab_id
    # for index, row in button_id_function_mapping.iterrows():
    #     button_id = row['button_id']
    #     button_name = button_definition.loc[button_id]['text']
    #     button_id_function_mapping.loc[index, 'button_id'] = button_name

    # for button in button_attributes:
    #     button_name = button['text']
    #     button_function_id_df = button_id_function_mapping.loc[button_id_function_mapping['button_id'] == button_name]
    #     button_function_id = list(button_function_id_df['function_id'])
    #     button['functions'] = []
    #     # Add all functions
    #     for function_id in button_function_id:
    #         function_id_df = button_functions_df.loc[function_id]
    #         function = function_id_df.to_dict()
    #         function['parameters'] = function['parameters'].split(',') # Send list of parameters instead of string
    #         button['functions'].append(function)

    # buttons = list(buttons_df['text'])
    # print('Time taken for button functions',time()-button_time)
    buttons = button_functions(db,invoice_files_df,columns)
    # print(f'INFO: - Buttons: {buttons}')

    # * FIELDS
    # Get field IDs for the queue field mapping
    # fieldid_time = time()
    # queue_field_mapping = db.get_all('queue_field_mapping')
    # queue_id_field_mapping = queue_field_mapping.loc[queue_field_mapping['queue_id'] == queue_id]

    # field_ids = list(db.execute(query, params = [queue_id]))
    # field_ids = list(queue_id_field_mapping['field_id'])

    # Filter out some field IDs using the rule
    # rule_group_list = list(queue_id_field_mapping.rule)
    # for index, group in enumerate(rule_group_list):
    #     # Apply business rule
    #     # If business returns False, pop the id from field_ids
    #     pass

    # Get field definition corresponding the field IDs


    # field_definition = db.get_all('field_definition')
    # query = "SELECT field_id from queue_field_mapping where queue_id = %s"
    # query = f"SELECT * from field_definition where id in ({query})"
    # fields_df = db.execute(query, params=[queue_id])
    # # fields_df = field_definition.ix[field_ids]
    # print('Time taken for fetching fields ids',time()-fieldid_time)
    fields_df = fetch_field_ids(db)
    # Get tab definition
    tab_definition = db.get_all('tab_definition')

    # extraction_db = DB('extraction')

    # Replace tab_id in fields with the actual tab names
    # Also create unique name for the buttons by combining display name
    # and tab name
    # datasrc_time = time()
    # excel_display_data = {}
    # tab_type_mapping = {}
    # for index, row in fields_df.iterrows():
    #     tab_id = row['tab_id']
    #     tab_name = tab_definition.loc[tab_id]['text']
    #     tab_source = tab_definition.loc[tab_id]['source']
    #     tab_type = tab_definition.loc[tab_id]['type']
    #     fields_df.loc[index, 'tab_id'] = tab_name

    #     tab_type_mapping[tab_name] = tab_type

    #     if tab_type == 'excel':
    #         source_table_name = tab_source + '_source'

    #         # Get excel source data and convert it to dictionary
    #         excel_source_data = extraction_db.get_all(source_table_name)

    #         if tab_name not in excel_display_data:
    #             excel_display_data[tab_name] = {
    #                 'column': list(excel_source_data),
    #                 'data': excel_source_data.to_dict(orient='records')[:100]
    #             }
    # print('Time taken for datasrc stuff',time()-datasrc_time)
    source_table_name, excel_display_data = datasrc_stuff(extraction_db, fields_df)
    # field_attributes = fields_df.to_dict(orient='records')
    # tabs = list(fields_df.tab_id.unique())
    # # print(f'INFO: - Fields: {fields}')

    # if 'cluster' not in columns:
    #     columns.append('last_updated_by')
    #     columns.append('vendor_name')

    # if end_point > total_files:
    #     end_point = total_files

    # pagination = {"start": start_point + 1, "end": end_point, "total": total_files}

    # query = "SELECT id, username from users where role = 'TL_Prod'"
    # retrain_access = operator in list(user_db.execute(query).username)

    # if queue_name == 'Verify' and retrain_access:
    #     retrain = 1
    # else:
    #     retrain = 0

    # data = {
    #     'columns': columns,
    #     'files': files,
    #     'buttons': button_attributes,
    #     'field': field_attributes,
    #     'tabs': tabs,
    #     'excel_source_data': excel_display_data,
    #     'tab_type_mapping': tab_type_mapping,
    #     'pagination': pagination,
    #     'retrain': retrain
    # }
    # print('Total time taken in get_queue',time()-rt_time)
    data = fetch_data(user_db,fields_df,columns, total_files)
    return jsonify({'flag': True, 'data': data})

@app.route('/get_display_fields/<case_id>', methods=['POST', 'GET'])
def get_display_fields(case_id=None):
    # ! MAKE THIS ROUTE AFTER THE PREVIOUS ROUTE IS STABLE
    data = request.json

    queue_id = data.pop('queue_id', None)

    if queue_id is None:
        message = f'Queue ID not provided.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    if case_id is None:
        message = f'Case ID not provided.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    # Get queue name using queue ID
    queue_definition = db.get_all('queue_definition')

    # * BUTTONS
    # Get workflow definition for the selected queue ID
    workflow_definition = db.get_all('workflow_definition')
    queue_workflow = workflow_definition.loc[workflow_definition['queue_id'] == queue_id]

    # Get button IDs for the queue ID
    button_ids = list(queue_workflow['button_id'])

    # Get buttons' attributes from button definition
    button_definition = db.get_all('button_definition')
    buttons_df = button_definition.ix[button_ids]
    button_attributes = buttons_df.to_dict(orient='records')

    # Add which queue to move to in button attributes
    raw_move_to_ids = list(queue_workflow['move_to'])
    move_to_ids = [id if id is not None else -1 for id in raw_move_to_ids]
    move_to_df = queue_definition.ix[move_to_ids]
    move_to = list(move_to_df['name'])
    for index, button in enumerate(button_attributes):
        if move_to[index] != -1:
            button['move_to'] = move_to[index]

    # Get button functions
    button_functions_df = db.get_all('button_functions')
    button_function_mapping = db.get_all('button_function_mapping')
    button_id_function_mapping = button_function_mapping.loc[button_function_mapping['button_id'].isin(button_ids)]
    # TODO: Convert this loop into a function. Using it later again for tab_id
    for index, row in button_id_function_mapping.iterrows():
        button_id = row['button_id']
        button_name = button_definition.loc[button_id]['text']
        button_id_function_mapping.loc[index, 'button_id'] = button_name

    for button in button_attributes:
        button_name = button['text']
        button_function_id_df = button_id_function_mapping.loc[button_id_function_mapping['button_id'] == button_name]
        button_function_id = list(button_function_id_df['function_id'])
        button['functions'] = []
        # Add all functions
        for function_id in button_function_id:
            function_id_df = button_functions_df.loc[function_id]
            function = function_id_df.to_dict()
            function['parameters'] = function['parameters'].split(',') # Send list of parameters instead of string
            button['functions'].append(function)

    buttons = list(buttons_df['text'])
    print(f'INFO: - Buttons: {buttons}')

    # * FIELDS
    # Get field IDs for the queue field mapping
    queue_field_mapping = db.get_all('queue_field_mapping')
    queue_id_field_mapping = queue_field_mapping.loc[queue_field_mapping['queue_id'] == queue_id]
    field_ids = list(queue_id_field_mapping['field_id'])

    # Get field definition corresponding the field IDs
    field_definition = db.get_all('field_definition')
    fields_df = field_definition.ix[field_ids]
    fields_df['unique_name'] = Series('', index=fields_df.index)

    # Get tab definition
    tab_definition = db.get_all('tab_definition')

    # Replace tab_id in fields with the actual tab names
    # Also create unique name for the buttons by combining display name
    # and tab name
    for index, row in fields_df.iterrows():
        tab_id = row['tab_id']
        tab_name = tab_definition.loc[tab_id]['text']
        fields_df.loc[index, 'tab_id'] = tab_name

        formate_display_name = row['display_name'].lower().replace(' ', '_')
        unique_name = f'{formate_display_name}_{tab_name.lower()}'.replace(' ', '_')
        fields_df.loc[index, 'unique_name'] = unique_name

    field_attributes = fields_df.to_dict(orient='records')
    fields = list(fields_df.display_name.unique())
    tabs = list(fields_df.tab_id.unique())
    # print(f'INFO: - Fields: {fields}')

    response_data = {
        'buttons': button_attributes,
        'field': field_attributes,
        'tabs': tabs
    }

    return jsonify({'flag': True, 'data': response_data})

@zipkin_span(service_name='get_fields', span_name='extract_ocr')
def extract_ocr(queue_db, case_id):
    try:
        query = "SELECT id, ocr_data from ocr_info where case_id = %s"
        ocr_data = queue_db.execute(query,params=[case_id])
        # ocr_data = ocr_data[ocr_data['case_id'] == case_id]
        ocr_data = list(ocr_data['ocr_data'])[0]
    except Exception as e:
        ocr_data = '{}'
        print(f'ERROR: {e}')
        print(f'Error in extracting ocr from db')
        pass

    return ocr_data

@zipkin_span(service_name='get_fields', span_name='update_ocr')
def update_ocr(queue_db, operator, case_id):    
    update = {
        'operator': operator
    }
    where = {
        'case_id': case_id
    }

#    oper_time = time()
    queue_db.update('process_queue', update=update, where=where)
#    print('Time taken to update operator',time()-oper_time)
    
@zipkin_span(service_name='get_fields', span_name='get_qids_exception')
def get_qids_exception(queue_db, queue_name):

    queue_definition = queue_db.get_all('queue_definition')
    queue_info = queue_definition.loc[queue_definition['name'] == queue_name]
    queue_id = queue_definition.index[queue_definition['name'] == queue_name].tolist()[0]
#    print('Time taken for getting qid basis exception type',time()-qid_st)

    return queue_info, queue_id

@zipkin_span(service_name='get_fields', span_name='get_field_ids')
def get_field_ids(queue_db, queue_id):
    queue_field_mapping = queue_db.get_all('queue_field_mapping')
    queue_id_field_mapping = queue_field_mapping.loc[queue_field_mapping['queue_id'] == queue_id]
    field_ids = list(queue_id_field_mapping['field_id'])

    return field_ids

@zipkin_span(service_name='get_fields', span_name='dropdown_stuff')
def dropdown_stuff(queue_db, field_ids):
    dropdown_definition = queue_db.get_all('dropdown_definition')
    field_dropdown = dropdown_definition.loc[dropdown_definition['field_id'].isin(field_ids)] # Filter using only field IDs from the file
    unique_field_ids = list(field_dropdown.field_id.unique()) # Get unique field IDs from dropdown definition
    field_definition = queue_db.get_all('field_definition')
    dropdown_fields_df = field_definition.ix[unique_field_ids] # Get field names using the unique field IDs
    dropdown_fields_names = list(dropdown_fields_df.unique_name)

    dropdown = {}
    for index, f_id in enumerate(unique_field_ids):
        dropdown_options_df = field_dropdown.loc[field_dropdown['field_id'] == f_id]
        dropdown_options = list(dropdown_options_df.dropdown_option)
        dropdown[dropdown_fields_names[index]] = dropdown_options

    return dropdown, field_definition

@zipkin_span(service_name='get_fields', span_name='get_highlights')
def get_highlights(extraction_db, case_id):
    # ocr_fields = extraction_db.get_all('ocr')
    query = "SELECT `id`, `highlight`, `Table` from `ocr` where `case_id` = %s ORDER BY `created_date` DESC Limit 1"
    # latest_ocr_fields = extraction_db.get_latest(ocr_fields, 'case_id', 'created_date')
    # case_id_ocr = latest_ocr_fields.loc[latest_ocr_fields['case_id'] == case_id]
    case_id_ocr = extraction_db.execute(query, params = [case_id])
    highlight = json.loads(list(case_id_ocr['highlight'])[0])
    table = list(case_id_ocr['Table'])[0]
    
    return highlight, case_id_ocr, table

@zipkin_span(service_name='get_fields', span_name='remaining_fields')
def remaining_fields(extraction_db, fields_df, tab_definition, case_id, highlight):
    renamed_fields = {}
    renamed_higlight = {}

    field_source_data = {}

    for index, row in fields_df.to_dict('index').items():
#        for_time = time()
        tab_id = row['tab_id']
        tab_name = tab_definition.loc[tab_id]['text']
        table_name = tab_definition.loc[tab_id]['source']
        fields_df.loc[index, 'tab_id'] = tab_name

        display_name = row['display_name']
        unique_name = row['unique_name']

        # Get data related to the case from table for the corresponding tab
#        get_all_time = time()
#        query_time = time()
        # tab_files_df = extraction_db.get_all(table_name)
        # print('Getall time:',time()-get_all_time)

        # latest_tab_files = queue_db.get_latest(tab_files_df, 'case_id', 'created_date')
        # case_tab_files = latest_tab_files.loc[latest_tab_files['case_id'] == case_id]

        # Check if such table exists. If not then skip tab
        try:
            if table_name not in field_source_data:
                get_fieldsinfo_q = f"SELECT * FROM `{table_name}` WHERE case_id='{case_id}' ORDER BY created_date Desc Limit 1"
                # print(f'{table_name} not in field source. Adding.')
                field_source_data[table_name] = extraction_db.execute(get_fieldsinfo_q)

            case_tab_files = field_source_data[table_name]
            if case_tab_files is False:
                message = f'No table named `{table_name}`.'
                print(f'ERROR:  - {message}')
                continue
            # case_tab_files = queue_db.get_latest(tab_files_df, 'case_id', 'created_date')
            # query_time_list.append(time()-query_time)
            # print(f' - # {index} Query time: {time()-query_time}')

            if case_tab_files.empty:
                message = f' - No such case ID `{case_id}` in `{table_name}`.'
                print(f'ERROR: {message}')
                continue
        except Exception as e:
            print('Exception in get fields',e)
            continue

        case_files_filtered = case_tab_files.loc[:, 'created_date':] # created_date column will be included
        fields_df = case_files_filtered.drop(columns='created_date') # Drop created_date column
        table_fields_ = fields_df.to_dict(orient='records')[0] # Get corresponding table fields

        if display_name in table_fields_:
            renamed_fields[unique_name] = table_fields_[display_name]

        if display_name in highlight and table_name == 'ocr':
            renamed_higlight[unique_name] = highlight[display_name]
            
    return renamed_fields, renamed_higlight

@app.route('/get_fields', methods=['POST', 'GET'])
@app.route('/get_fields/<case_id>', methods=['POST', 'GET'])
def get_fields(case_id=None):
    with zipkin_span(service_name='get_fields', span_name='case_open', 
            transport_handler=http_transport, sample_rate=100,):
        data = request.json
    
        operator = data.pop('user', None)
    
        if operator is None:
            message = f'Operator name not provided.'
            print(f'ERROR: {message}')
            return jsonify({'flag': False, 'message': message})
    
        if case_id is None:
            message = f'Case ID not provided.'
            print(f'ERROR: {message}')
            return jsonify({'flag': False, 'message': message})
    
        queue_db_config = {
            'host': 'queue_db',
            'port': 3306,
            'user': 'root',
            'password': 'root'
        }
        queue_db = DB('queues', **queue_db_config)
        # queue_db = DB('queues')
    
        extraction_db_config = {
            'host': 'extraction_db',
            'port': 3306,
            'user': 'root',
            'password': 'root'
        }
        extraction_db = DB('extraction', **extraction_db_config)
        # extraction_db = DB('extraction')
    
        # Get tab definition
        tab_definition = queue_db.get_all('tab_definition')
    
        # Get queue ID using exception type from case files in process_queue table
#        qid_st = time()
        # files_df = queue_db.get_all('process_queue',discard=['ocr_data','ocr_text','xml_data'])
        # print(ocr_data)
#        try:
#            query = "SELECT id, ocr_data from ocr_info where case_id = %s"
#            ocr_data = queue_db.execute(query,params=[case_id])
#            # ocr_data = ocr_data[ocr_data['case_id'] == case_id]
#            ocr_data = list(ocr_data['ocr_data'])[0]
#        except Exception as e:
#            ocr_data = '{}'
#            print(f'ERROR: {e}')
#            print(f'Error in extracting ocr from db')
#            pass
        print("Case ID : ",case_id, type(case_id))
        ocr_data = extract_ocr(queue_db,case_id)
    
        # ocr_data = list(ocr_data.ocr_data)[0]
        # latest_case_file = queue_db.get_latest(files_df, 'case_id', 'created_date')
        query = "SELECT id, file_name, queue, operator from process_queue where case_id = %s"
        # latest_case_file = files_df
        # case_files = latest_case_file.loc[latest_case_file['case_id'] == case_id]
        case_files = queue_db.execute(query, params = [case_id])
        case_operator = list(case_files.operator)[0]
        file_name = list(case_files.file_name)[0]
        queue_name = list(case_files.queue)[0]
    
        if case_files.empty:
            message = f'No case ID `{case_id}` found in process queue.'
            print(f'ERROR: {message}')
            return jsonify({'flag': False, 'message': message})
        else:
            if queue_name == 'Failed':
                message = 'Just display the image'
                return jsonify({'flag': True, 'message': message, 'corrupted': True, 'file_name':file_name})
    
        if case_operator is not None and case_operator != operator:
            message = f'This file/cluster is in use by the user `{case_operator}`.'
            print(f'ERROR: {message}')
            return jsonify({'flag': False, 'message': message})
    
        # Lock the file and assign it to the operator
#        update = {
#            'operator': operator
#        }
#        where = {
#            'case_id': case_id
#        }
#    
#        oper_time = time()
#        queue_db.update('process_queue', update=update, where=where)
#        print('Time taken to update operator',time()-oper_time)
        update_ocr(queue_db, operator, case_id)
#    
#        queue_definition = queue_db.get_all('queue_definition')
#        queue_info = queue_definition.loc[queue_definition['name'] == queue_name]
#        queue_id = queue_definition.index[queue_definition['name'] == queue_name].tolist()[0]
#        print('Time taken for getting qid basis exception type',time()-qid_st)
    
        # Get field related to case ID from queue_field_mapping
#        fields_ids_time = time()
#        queue_field_mapping = queue_db.get_all('queue_field_mapping')
#        queue_id_field_mapping = queue_field_mapping.loc[queue_field_mapping['queue_id'] == queue_id]
#        field_ids = list(queue_id_field_mapping['field_id'])
#        print('Time taken for getting fields ids',time()-fields_ids_time)
        queue_info, queue_id = get_qids_exception(queue_db, queue_name)
        field_ids = get_field_ids(queue_db, queue_id)
        # Get dropdown values using field IDs which have dropdown values dropdown_definition
#        dropdown_time = time()
#        dropdown_definition = queue_db.get_all('dropdown_definition')
#        field_dropdown = dropdown_definition.loc[dropdown_definition['field_id'].isin(field_ids)] # Filter using only field IDs from the file
#        unique_field_ids = list(field_dropdown.field_id.unique()) # Get unique field IDs from dropdown definition
#        field_definition = queue_db.get_all('field_definition')
#        dropdown_fields_df = field_definition.ix[unique_field_ids] # Get field names using the unique field IDs
#        dropdown_fields_names = list(dropdown_fields_df.unique_name)
#    
#        dropdown = {}
#        for index, f_id in enumerate(unique_field_ids):
#            dropdown_options_df = field_dropdown.loc[field_dropdown['field_id'] == f_id]
#            dropdown_options = list(dropdown_options_df.dropdown_option)
#            dropdown[dropdown_fields_names[index]] = dropdown_options
#        print('Time taken for dropdown stuff',time()-dropdown_time)

        dropdown, field_definition = dropdown_stuff(queue_db, field_ids)
    
        fields_df = field_definition.ix[field_ids] # Get field names using the unique field IDs
    
        # Get higlights
#        highlight_time = time()
#        # ocr_fields = extraction_db.get_all('ocr')
#        query = "SELECT `id`, `highlight`, `Table` from `ocr` where `case_id` = %s ORDER BY `created_date` DESC Limit 1"
#        # latest_ocr_fields = extraction_db.get_latest(ocr_fields, 'case_id', 'created_date')
#        # case_id_ocr = latest_ocr_fields.loc[latest_ocr_fields['case_id'] == case_id]
#        case_id_ocr = extraction_db.execute(query, params = [case_id])
#        highlight = json.loads(list(case_id_ocr['highlight'])[0])
#        table = list(case_id_ocr['Table'])[0]
#        print('time taken for getting highlights',time()-highlight_time)
#        
        highlight,case_id_ocr,table = get_highlights(extraction_db, case_id)
        # Renaming of fields
#        rename_time = time()
#        renamed_fields = {}
#        renamed_higlight = {}
#    
#        field_source_data = {}
#    
#        for index, row in fields_df.to_dict('index').items():
#            for_time = time()
#            tab_id = row['tab_id']
#            tab_name = tab_definition.loc[tab_id]['text']
#            table_name = tab_definition.loc[tab_id]['source']
#            fields_df.loc[index, 'tab_id'] = tab_name
#    
#            display_name = row['display_name']
#            unique_name = row['unique_name']
#    
#            # Get data related to the case from table for the corresponding tab
#            get_all_time = time()
#            query_time = time()
#            # tab_files_df = extraction_db.get_all(table_name)
#            # print('Getall time:',time()-get_all_time)
#    
#            # latest_tab_files = queue_db.get_latest(tab_files_df, 'case_id', 'created_date')
#            # case_tab_files = latest_tab_files.loc[latest_tab_files['case_id'] == case_id]
#    
#            # Check if such table exists. If not then skip tab
#            try:
#                if table_name not in field_source_data:
#                    get_fieldsinfo_q = f"SELECT * FROM `{table_name}` WHERE case_id='{case_id}' ORDER BY created_date Desc Limit 1"
#                    # print(f'{table_name} not in field source. Adding.')
#                    field_source_data[table_name] = extraction_db.execute(get_fieldsinfo_q)
#    
#                case_tab_files = field_source_data[table_name]
#                if case_tab_files is False:
#                    message = f'No table named `{table_name}`.'
#                    print(f'ERROR:  - {message}')
#                    continue
#                # case_tab_files = queue_db.get_latest(tab_files_df, 'case_id', 'created_date')
#                # query_time_list.append(time()-query_time)
#                # print(f' - # {index} Query time: {time()-query_time}')
#    
#                if case_tab_files.empty:
#                    message = f' - No such case ID `{case_id}` in `{table_name}`.'
#                    print(f'ERROR: {message}')
#                    continue
#            except Exception as e:
#                print('Exception in get fields',e)
#                continue
#    
#            case_files_filtered = case_tab_files.loc[:, 'created_date':] # created_date column will be included
#            fields_df = case_files_filtered.drop(columns='created_date') # Drop created_date column
#            table_fields_ = fields_df.to_dict(orient='records')[0] # Get corresponding table fields
#    
#            if display_name in table_fields_:
#                renamed_fields[unique_name] = table_fields_[display_name]
#    
#            if display_name in highlight and table_name == 'ocr':
#                renamed_higlight[unique_name] = highlight[display_name]
#    
#            # timespent = list(case_files.time_spent)[0]
#            # h, m, s = timespent.split(':')
#            # timespent_in_secs = (int(h) * 3600) + (int(m) * 60) + int(s)
#            # print('Field#',index,'Time:',time()-for_time)
#        print('Time taken for renaming fields',time()-rename_time)
        renamed_fields, renamed_higlight = remaining_fields(extraction_db, fields_df, tab_definition, case_id, highlight)
        # print("Table:", table)
        if table != '[]':
            if table:
                table = [ast.literal_eval(table)]
        else:
            table = []
    
        response_data = {
            'flag': True,
            'data': renamed_fields,
            'dropdown_values': dropdown,
            'highlight': renamed_higlight,
            'file_name': file_name,
            'table': table,
            'time_spent': 0,
            'timer': list(queue_info.timer)[0],
            'ocr_data': ocr_data
        }
    
#        print('Total time taken for opeining file',time()-st_time)
    
        return jsonify(response_data)

@app.route('/refresh_fields', methods=['POST', 'GET'])
def refresh_fields(case_id=None):
    data = request.json

    case_id = data.pop('case_id')

    if case_id is None:
        message = f'Case ID not provided.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    queue_db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    queue_db = DB('queues', **queue_db_config)
    # queue_db = DB('queues')

    extraction_db_config = {
        'host': 'extraction_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    extraction_db = DB('extraction', **extraction_db_config)
    # extraction_db = DB('extraction')

    # Get tab definition
    tab_definition = queue_db.get_all('tab_definition')

    # Get queue ID using exception type from case files in process_queue table
    files_df = queue_db.get_all('process_queue')
    # latest_case_file = queue_db.get_latest(files_df, 'case_id', 'created_date')
    latest_case_file = files_df
    case_files = latest_case_file.loc[latest_case_file['case_id'] == case_id]

    if case_files.empty:
        message = f'No case ID `{case_id}` found in process queue.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    queue_name = list(case_files['queue'])[0]
    queue_definition = queue_db.get_all('queue_definition')
    queue_info = queue_definition.loc[queue_definition['name'] == queue_name]
    queue_id = queue_definition.index[queue_definition['name'] == queue_name].tolist()[0]

    # Get field related to case ID from queue_field_mapping
    queue_field_mapping = queue_db.get_all('queue_field_mapping')
    queue_id_field_mapping = queue_field_mapping.loc[queue_field_mapping['queue_id'] == queue_id]
    field_ids = list(queue_id_field_mapping['field_id'])
    field_definition = queue_db.get_all('field_definition')

    fields_df = field_definition.ix[field_ids] # Get field names using the unique field IDs

    # Renaming of fields
    renamed_fields = {}
    for index, row in fields_df.iterrows():
        tab_id = row['tab_id']
        tab_name = tab_definition.loc[tab_id]['text']
        table_name = tab_definition.loc[tab_id]['source']
        fields_df.loc[index, 'tab_id'] = tab_name

        display_name = row['display_name']
        unique_name = row['unique_name']

        # Get data related to the case from table for the corresponding tab
        tab_files_df = extraction_db.get_all(table_name)

        # Check if such table exists. If not then skip tab
        if tab_files_df is False:
            message = f'No table named `{table_name}`.'
            print(f'ERROR:  - {message}')
            continue

        # latest_tab_files = queue_db.get_latest(tab_files_df, 'case_id', 'created_date')
        latest_tab_files = tab_files_df
        case_tab_files = latest_tab_files.loc[latest_tab_files['case_id'] == case_id]
        if case_tab_files.empty:
            message = f' - No such case ID `{case_id}` in `{table_name}`.'
            print(f'ERROR: {message}')
            continue
        case_files_filtered = case_tab_files.loc[:, 'created_date':] # created_date column will be included
        fields_df = case_files_filtered.drop(columns='created_date') # Drop created_date column
        table_fields_ = fields_df.to_dict(orient='records')[0] # Get corresponding table fields

        if display_name in table_fields_:
            renamed_fields[unique_name] = table_fields_[display_name]


    response_data = {
        'flag': True,
        'updated_fields_dict': renamed_fields,
        'message': "Successfully applied all validations"
    }

    return jsonify(response_data)

@app.route('/unlock_case', methods=['POST', 'GET'])
def unlock_case():
    data = request.json

    case_id = data.pop('case_id', None)
    time_spent = data.pop('time_spent', None)

    if case_id is None:
        message = f'Case ID not provided.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    queue_db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    queue_db = DB('queues', **queue_db_config)
    # queue_db = DB('queues')

    # Get queue ID using exception type from case files in process_queue table
    files_df = queue_db.get_all('process_queue',discard=['ocr_data','ocr_text','xml_data'])
    # latest_case_file = queue_db.get_latest(files_df, 'case_id', 'created_date')
    latest_case_file = files_df
    case_files = latest_case_file.loc[latest_case_file['case_id'] == case_id]
    operator = list(case_files.operator)[0]

    time_spent = int(time_spent)
    h = time_spent // 3600
    m = (time_spent % 3600) // 60
    s = time_spent % 60

    # Update the time spent on the particular file
    update = {
        'operator': None,
        'time_spent': f'{h:02d}:{m:02d}:{s:02d}',
        'last_updated_by': operator
    }
    where = {
        'operator': operator
    }
    queue_db.update('process_queue', update=update, where=where)

    # Update the operator to None on the files in the same cluster
    update = {
        'operator': None
    }
    where = {
        'cluster': list(case_files.cluster)[0]
    }
    queue_db.update('process_queue', update=update, where=where)

    return jsonify({'flag': True, 'message': 'Unlocked file.'})

@app.route('/get_ocr_data', methods=['POST', 'GET'])
def get_ocr_data():
    data = request.json

    case_id = data['case_id']
    try:
        retrain = data['retrain']
    except:
        retrain = ''

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    trained_db_config = {
        'host': 'template_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    trained_db = DB('template_db', **trained_db_config)

    extarction_db_config = {
        'host': 'extraction_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    extraction_db = DB('extraction', **extarction_db_config)

    table_db_config = {
        'host': 'table_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    table_db = DB('table_db', **table_db_config)


    print("Case ID:", case_id)

    # Get all OCR mandatory fields
    try:
        tab_df = db.get_all('tab_definition')
        ocr_tab_id = tab_df.index[tab_df['source'] == 'ocr'].tolist()[0]

        query = 'SELECT * FROM `field_definition` WHERE `tab_id`=%s'
        ocr_fields_df = db.execute(query, params=[ocr_tab_id])
        mandatory_fields = list(ocr_fields_df.loc[ocr_fields_df['mandatory'] == 1]['display_name'])
    except Exception as e:
        print(f'Error getting mandatory fields: {e}')
        mandatory_fields = []

    # Get data related to the case from invoice table
    invoice_files_df = db.get_all('process_queue')

    case_files = invoice_files_df.loc[invoice_files_df['case_id'] == case_id]
    if case_files.empty:
        message = f'No such case ID {case_id}.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
    params = [case_id]
    ocr_info = db.execute(query, params=params)
    ocr_data = json.loads(list(ocr_info.ocr_data)[0])

    ocr_data = [sort_ocr(data) for data in ocr_data]

    vendor_list = list(trained_db.get_all('vendor_list').vendor_name)
    template_list = list(trained_db.get_all('trained_info').template_name)

    if retrain.lower() == 'yes':
        template_name = list(case_files['template_name'])[0]
        # print('template_name',template_name)
        # '''Fetch fields train info from database'''
        trained_info = trained_db.get_all('trained_info')
        trained_info = trained_info.loc[trained_info['template_name'] == template_name]
        field_data = json.loads(list(trained_info.field_data)[0])
        # Fetch Table train info from database
        table_train_info = table_db.get_all('table_info')
        table_train_info = table_train_info.loc[table_train_info['template_name'] == template_name]
        try:
            table_info = json.loads(list(table_train_info.table_data)[0])
        except:
            table_info = {}
        extraction_ocr = extraction_db.get_all('ocr')
        extraction_ocr = extraction_ocr.loc[extraction_ocr['case_id'] == case_id]
        highlight = json.loads(list(extraction_ocr.highlight)[0])

        fields_info = get_fields_info(ocr_data,highlight,field_data)

        return jsonify({'flag': True,
            'data': ocr_data,
            'info': {
                'fields': fields_info,
                'table': table_info
            },
            'template_name': template_name,
            'vendor_list': vendor_list,
            'template_list': template_list,
            'mandatory_fields': mandatory_fields})

    return jsonify({'flag': True, 'data': ocr_data, 'vendor_list': vendor_list, 'template_list': template_list, 'mandatory_fields': mandatory_fields})

@app.route('/update_queue', methods=['POST', 'GET'])
def update_queue():
    data = request.json

    try:
        verify_operator = data['operator']
    except:
        print("operator key not found")
        verify_operator = None

    # print(data.keys())

    if 'case_id' not in data or 'queue' not in data or 'fields' not in data:
        message = f'Invalid JSON recieved. MUST contain `case_id`, `queue` and `fields` keys.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    case_id = data['case_id']
    queue = data['queue']
    # print("Move to:", queue)
    fields = data['fields']

    # print(fields)

    if data is None or not data:
        message = f'Data not provided/empty dict.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    if case_id is None or not case_id:
        message = f'Case ID not provided/empty string.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    if queue is None or not queue:
        message = f'Queue not provided/empty string.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    # Get latest data related to the case from invoice table
    invoice_files_df = db.get_all('process_queue')
    # latest_case_file = db.get_latest(invoice_files_df, 'case_id', 'created_date')
    latest_case_file = invoice_files_df
    case_files = latest_case_file.loc[latest_case_file['case_id'] == case_id]

    if case_files.empty:
        message = f'No case ID `{case_id}` found in process queue.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    # case_queue_files = case_files.loc[case_files['queue'] == queue]
    # case_files_dict = case_files.to_dict(orient='records')[0]

    # If the file is already in the queue previously, update the values
    # Else insert a new record with new queue and values
    # if not case_queue_files.empty:
    #     message = f'Case ID `{case_id}` is already in `{queue}`. Deleting the record.'
    #     print(f'INFO:{message}')

    #     query = 'DELETE FROM `process_queue` WHERE `case_id`=%s ORDER BY `created_date` DESC LIMIT 1'
    #     delete_status = db.execute(query, params=[case_id])
    #     delete_status = True
    #     if delete_status:
    #         print(f'INFO: - Deleted latest record.')
    #     else:
    #         message = f'Something went wrong deleting the latest record for case `{case_id}`. Check logs.'
    #         print(f'ERROR:  - {message}')
    #         return jsonify({'flag': False, 'message': message})

    # message = f'Inserting new record in process_queue for case ID `{case_id}` with queue `{queue}`.'
    # print(f'->  {message}')

    # case_files_dict.pop('created_date', None) # Exclude created date column because it updates automatically
    # case_files_dict['queue'] = queue # Update the queue name

    # column_names = []
    # params = []
    # for column, value in case_files_dict.items():
    #     column_names.append(f'`{column}`')
    #     params.append(value)

    # query_column_names = ', '.join(column_names)
    # query_values_placeholder = ', '.join(['%s'] * len(params))

    # query = f'INSERT INTO `process_queue` ({query_column_names}) VALUES ({query_values_placeholder})'
    # insert_status = True
    # insert_status = db.execute(query, params=params)

    query = f'UPDATE `process_queue` SET `queue`=%s WHERE `case_id`=%s'
    params = [queue, case_id]
    update_status = db.execute(query, params=params)

    if update_status:
        message = f'Updated queue for case ID `{case_id}` successfully.'
        print(f' -> {message}')
    else:
        message = f'Something went wrong updating queue. Check logs.'
        print(f'ERROR: - {message}')
        return jsonify({'flag': False, 'message': message})

    # ! UPDATE TRACE INFO TABLE HERE
    update_queue_trace(db,case_id,queue)

    # Inserting fields into respective tables
    field_definition = db.get_all('field_definition')
    tab_definition = db.get_all('tab_definition')

    # Change tab ID to its actual names
    for index, row in field_definition.iterrows():
        tab_id = row['tab_id']
        tab_name = tab_definition.loc[tab_id]['text']
        field_definition.loc[index, 'tab_id'] = tab_name

    # Create a new dictionary with key as table, and value as fields dict (column name: value)
    table_fields = {}
    for unique_name, value in fields.items():
        unique_field_name = field_definition.loc[field_definition['unique_name'] == unique_name]

        if unique_field_name.empty:
            print(f'No unique field name for {unique_name}. Check `field_defintion` database.')
            continue

        table = list(unique_field_name.tab_id)[0].lower().replace(' ', '_')
        display_name = list(unique_field_name.display_name)[0]

        if table not in table_fields:
            table_fields[table] = {}

        table_fields[table][display_name] = value

    extraction_db_config = {
        'host': 'extraction_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    extraction_db = DB('extraction', **extraction_db_config)
    # extraction_db = DB('extraction')

    # ! GET HIGHLIGHT FROM PREVIOUS RECORD BECAUSE UI IS NOT SENDING
    ocr_files_df = extraction_db.get_all('ocr')
    latest_ocr_files = extraction_db.get_latest(ocr_files_df, 'case_id', 'created_date')
    ocr_case_files = latest_ocr_files.loc[latest_ocr_files['case_id'] == case_id]
    highlight = list(ocr_case_files.highlight)[0]

    for table_name, fields_dict in table_fields.items():
        # Only in OCR table add the highlight
        if table_name == 'ocr':
            column_names = ['`case_id`', '`highlight`']
            params = [case_id, highlight]
        else:
            column_names = ['`case_id`']
            params = [case_id]

        for column, value in fields_dict.items():
            if column == 'Verify Operator':
                column_names.append(f'`{column}`')
                params.append(verify_operator)
            else:
                column_names.append(f'`{column}`')
                params.append(value)
        query_column_names = ', '.join(column_names)
        query_values_placeholder = ', '.join(['%s'] * len(params))

        query = f'INSERT INTO `{table_name}` ({query_column_names}) VALUES ({query_values_placeholder})'
        print(f'INFO:\nInserting into {table_name}')

        extraction_db.execute(query, params=params)

    return jsonify({'flag': True, 'message': 'Changing queue completed.'})

# @app.route('/execute_button_function', methods=['POST', 'GET'])
# def execute_button_function():
#     functions = request.json
#     message = None
#     updated_fields_dict = None
#     status_type = None

#     if functions is None or not functions:
#         message = f'Data recieved is none/empty. No function to execute.'
#         print(f'ERROR: {message}')
#         return jsonify({'flag': False, 'message': message})

#     for function in functions:
#         host = 'service_bridge'
#         port = 5002
#         data = function['parameters']
#         route = function['route']
#         response = requests.post(f'http://{host}:{port}/{route}', json=data)
#         response_data = response.json()

#         print("FUNCTION", route)

#         if not response_data['flag']:
#             try:
#                 message = response_data['message']
#             except:
#                 message = f'Failed during execute of route `{route}`. Check logs.'
#             print(f'ERROR: {message}')
#             return jsonify({'flag': False, 'message': message})
#         else:
#             if 'message' in response_data:
#                 message = response_data['message']
#             if 'updated_fields_dict' in response_data:
#                 updated_fields_dict = response_data['updated_fields_dict']
#             if 'status_type' in response_data:
#                 status_type = response_data['status_type']

#     if message is not None:
#         return jsonify({'flag': True, 'message': message, 'updated_fields_dict': updated_fields_dict, 'status_type': status_type})
#     else:
#         return jsonify({'flag': True, 'message': f'Succesfully executed functions', 'updated_fields_dict': updated_fields_dict, 'status_type': status_type})

@app.route('/get_queues', methods=['POST', 'GET'])
def get_queues():
    data = request.json

    if data is None or not data:
        message = f'Data recieved is none/empty.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    user_role = data.pop('role', None)

    # host = 'service_bridge'
    # port = 5002
    # route = 'verify_session'
    # session_response = requests.post(f'http://{host}:{port}/{route}', json=data)
    # session_response_data = session_response.json()

    # if not session_response_data['flag']:
    #     message = session_response_data['message']
    #     print(f'ERROR: {message}')
    #     return jsonify(session_response_data)

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    # Get queue name using queue ID
    queue_definition = db.get_all('queue_definition')
    queue_definition_dict = queue_definition.to_dict(orient='records')
    queue_ids = queue_definition.index.tolist()
    queues = []

    for index, definition in enumerate(queue_definition_dict):
        accesses = definition['access'].split(',')
        queue = {}
        if user_role in accesses:
            queue['name'] = definition['name']
            tokens = definition['name'].split()
            queue['path'] = tokens[0].lower() + ''.join(x.title() for x in tokens[1:]) if len(tokens) > 1 else tokens[0].lower()
            queue['pathId'] = queue_ids[index]
            queue['type'] = definition['type']
            queues.append(queue)

    if not user_role:
        return jsonify({'flag': False, 'message': 'logout'})

    if not queues:
        message = f'No queues available for role `{user_role}`.'
        print(f'ERROR: {message}')
        return jsonify({'flag': False, 'message': message})

    return jsonify({'flag': True, 'data': {'queues': queues}})

@app.route('/get_stats', methods=['POST', 'GET'])
def get_stats():
    data = request.json
    from_date = data['fromDate']
    to_date = data['toDate']

    bar = True

    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    extraction_db_config = {
        'host': 'extraction_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    extraction_db = DB('extraction', **extraction_db_config)
    # extraction_db = DB('extraction')

    all_st = time()
    process_queue_df_master = db.get_all('process_queue',discard=['ocr_data','ocr_text','xml_data'])
    print('Loading process queue',time()-all_st)

    if bar:
        # process_queue_df = db.get_all('process_queue')
        process_queue_df = process_queue_df_master

        if from_date:
            process_queue_filter = process_queue_df.loc[process_queue_df['created_date'] <= to_date]
            process_queue_filter = process_queue_filter.loc[process_queue_filter['created_date'] >= from_date]
        else:
            process_queue_filter = process_queue_df_master

    else:
        process_queue_df = process_queue_df_master
        process_queue_filter = process_queue_df_master
    sap_df = extraction_db.get_all('sap')
    business_rules_df = extraction_db.get_all('business_rule')

    # * Number of invoices uploaded. All unique case IDs.
    unique_case_ids = process_queue_df.case_id.unique()
    if not from_date:
        # latest_unique_cases = db.get_latest(process_queue_df, 'case_id', 'created_date')
        latest_unique_cases = process_queue_df
    else:
        # latest_unique_cases = db.get_latest(process_queue_df, 'case_id', 'created_date')
        latest_unique_cases = process_queue_df
        latest_unique_cases = latest_unique_cases.loc[latest_unique_cases['created_date'] <= to_date]
        latest_unique_cases = latest_unique_cases.loc[latest_unique_cases['created_date'] >= from_date]
    # latest_unique_cases = db.get_latest(process_queue_df, 'case_id', 'created_date')
    latest_unique_cases = process_queue_df
    num_of_invoice_uploaded = len(latest_unique_cases)

    # * Number of invoices pending. All queues except 'Approved' and 'Reject'.
    exclude_queue = ['Approved', 'Reject']
    if bar:
        invoices_pending_df = process_queue_df.loc[~process_queue_df['queue'].isin(exclude_queue)]
    else:
        invoices_pending_df = latest_unique_cases.loc[~latest_unique_cases['queue'].isin(exclude_queue)]
    num_of_invoice_pending = len(invoices_pending_df.case_id.unique())

    # * Number of invoices in 'Template Exceptions'.
    if bar:
        template_exceptions_current = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Template Exceptions']
        template_exceptions = process_queue_filter.loc[process_queue_filter['queue'] == 'Template Exceptions']
    else:
        template_exceptions = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Template Exceptions']
    num_of_template_exceptions = len(template_exceptions.case_id.unique())
    num_of_template_exceptions_current = len(template_exceptions_current)

    # * Number of invoices 'Approved'.
    if bar:
        invoices_approved_current = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Approved']
        invoices_approved_df = process_queue_filter.loc[process_queue_filter['queue'] == 'Approved']
    else:
        invoices_approved_df = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Approved']
    approved_ids = invoices_approved_df.case_id.unique()
    num_of_invoice_approved = len(approved_ids)

    # * Number of invoices 'Rejected'.
    if bar:
        invoices_rejected_df = process_queue_filter.loc[process_queue_filter['queue'] == 'Reject']
    else:
        invoices_rejected_df = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Reject']
    num_of_invoice_rejected = len(invoices_rejected_df.case_id.unique())

    # * Number of invoices 'Quality'.
    if bar:
        invoices_quality_current = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Quality Control']
        invoices_quality_df = process_queue_filter.loc[process_queue_filter['queue'] == 'Quality Control']
    else:
        invoices_quality_df = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Quality Control']
    num_of_invoice_quality = len(invoices_quality_df.case_id.unique())
    num_of_invoice_quality_current = len(invoices_quality_current)

    # * Number of invoices 'Verify'.
    invoices_verify_current = latest_unique_cases.loc[latest_unique_cases['queue'] == 'Verify']
    invoices_verify_df = process_queue_filter.loc[process_queue_filter['queue'] == 'Verify']
    verify_list = invoices_verify_df.case_id.unique()
    num_of_invoice_verify = len(verify_list)
    num_of_invoice_verify_current = len(invoices_verify_current)

    # * Number of invoices 'TL Verify'.
    if bar:
        invoices_tl_df = process_queue_filter.loc[process_queue_filter['queue'] == 'TL Verify']
        invoices_tl_current = latest_unique_cases.loc[latest_unique_cases['queue'] == 'TL Verify']
    else:
        invoices_tl_df = latest_unique_cases.loc[latest_unique_cases['queue'] == 'TL Verify']
    num_of_invoice_tl = len(invoices_tl_df.case_id.unique())
    num_of_invoice_tl_current = len(invoices_tl_current)

    ql_tl = num_of_invoice_quality + num_of_invoice_tl
    ql_tl_current = num_of_invoice_quality_current + num_of_invoice_tl_current

    # * Number of invoices processed and has gone through Team Lead.
    # tl_queues_df = queue_definition_df.loc[queue_definition_df['access'].str.contains('Team Lead')]
    # tl_queues = list(tl_queues_df.name)

    # # Awaiting permanent fix
    # tl_queues = ['TL Verify']

    # non_duplicates = process_queue_df.drop_duplicates(subset=['case_id', 'queue'])
    # tl_invoices = non_duplicates.loc[non_duplicates['queue'].isin(tl_queues)]
    # tl_invoices_latest = db.get_latest(tl_invoices, 'case_id', 'created_date')
    # tl_invoices_latest_approved = tl_invoices_latest.loc[tl_invoices_latest['queue'] == 'Approved']
    # num_of_invoice_processed_tl = len(tl_invoices_latest_approved)

    # * Successful/Unsuccessful SAP Inwards
    unique_sap_files = sap_df.drop_duplicates('case_id')
    successful_sap = unique_sap_files.loc[unique_sap_files['SAP Inward Status'].str.contains('Success', na=False)]
    unsuccessful_sap = unique_sap_files.loc[~unique_sap_files['SAP Inward Status'].str.contains('Success', na=True)]
    if not from_date:
        pass
    else:
        successful_sap = successful_sap.loc[successful_sap['created_date'] <= to_date]
        successful_sap = successful_sap.loc[successful_sap['created_date'] >= from_date]
        unsuccessful_sap = unsuccessful_sap.loc[unsuccessful_sap['created_date'] <= to_date]
        unsuccessful_sap = unsuccessful_sap.loc[unsuccessful_sap['created_date'] >= from_date]
    num_of_succssful_sap = len(successful_sap)
    num_of_unsuccssful_sap = len(unsuccessful_sap)

    # * Processed by TL IDs
    unique_business_rules_files = business_rules_df.drop_duplicates('case_id')
    tl_processed = unique_business_rules_files.loc[unique_business_rules_files['Verify Operator'].str.contains('P40000009', na=False)]
    ql_tl = len(tl_processed.case_id.unique())

    tl_list = tl_processed.case_id.unique()

    # * Manual from ACE
    manual_ace = unique_business_rules_files.loc[unique_business_rules_files['Bot Queue'].str.contains('No', na=False)]
    if not from_date:
        pass
    else:
        manual_ace = manual_ace.loc[manual_ace['created_date'] <= to_date]
        manual_ace = manual_ace.loc[manual_ace['created_date'] >= from_date]
    num_of_invoice_manual_ace = len(manual_ace)

    # Auto vs Manual Inward
    not_in = ['Verify', 'TL Verify', 'Quality Control']
    # if bar:
    #     process_queue_df = db.get_all('process_queue')
    #     if from_date:
    #         process_queue_df = process_queue_df.loc[process_queue_df['created_date'] <= to_date]
    #         process_queue_df = process_queue_df.loc[process_queue_df['created_date'] >= from_date]
    manual_approved = process_queue_filter.loc[(process_queue_filter['case_id'].isin(approved_ids)) & (process_queue_filter['queue'].isin(not_in))]
    manual_ids = manual_approved.case_id.unique()
    manual_inward = len(manual_ids)
    # manual_inward = 299
    auto_inward = num_of_invoice_approved - manual_inward


    one_month_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    three_month_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    combined_df = extraction_db.get_all('combined')

    unique_combined_files = combined_df.drop_duplicates('case_id')
    if not from_date:
        bot_processed = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('Yes', na=False)]
        bot_exception = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('No', na=False)]
    else:
        bot_processed = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('Yes', na=False)].loc[unique_combined_files['created_date'] <= to_date]
        bot_processed = bot_processed.loc[bot_processed['Bot Processed'].str.contains('Yes', na=False)].loc[bot_processed['created_date'] >= from_date]
        bot_exception = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('No', na=False)].loc[unique_combined_files['created_date'] <= to_date]
        bot_exception = bot_exception.loc[bot_exception['Bot Processed'].str.contains('No', na=False)].loc[bot_exception['created_date'] >= from_date]


    #Exceptions charts
    exceptions = {"No GRN": len(bot_exception.loc[bot_exception['Bot Exception'].str.contains('no grn', na=False)]),
                        "Reference Mismatch": len(bot_exception.loc[bot_exception['Bot Exception'].str.contains('reference mismatch', na=False)]),
                        "Balance greater than 1": len(bot_exception.loc[bot_exception['Bot Exception'].str.contains('balance greater than 1', na=False)]),
                        "Withholding Tax Tab": len(bot_exception.loc[bot_exception['Bot Exception'].str.contains('withholding tax tab', na=False)])}


    # bot_success_three = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('Yes', na=False)].loc[unique_combined_files['created_date'] > three_month_date]
    # bot_exception_three = unique_combined_files.loc[unique_combined_files['Bot Processed'].str.contains('No', na=False)].loc[unique_combined_files['created_date'] > three_month_date]

    # #Exceptions charts
    # all_time_exceptions = {"No GRN": len(bot_exception_alltime.loc[bot_exception_alltime['Bot Exception'].str.contains('no grn', na=False)]),
    #                     "Reference Mismatch": len(bot_exception_alltime.loc[bot_exception_alltime['Bot Exception'].str.contains('reference mismatch', na=False)]),
    #                     "Balance greater than 1": len(bot_exception_alltime.loc[bot_exception_alltime['Bot Exception'].str.contains('balance greater than 1', na=False)]),
    #                     "Withholding Tax Tab": len(bot_exception_alltime.loc[bot_exception_alltime['Bot Exception'].str.contains('withholding tax tab', na=False)])}

    # one_month_exceptions = {"No GRN": len(bot_exception_one.loc[bot_exception_one['Bot Exception'].str.contains('no grn', na=False)]),
    #                     "Reference Mismatch": len(bot_exception_one.loc[bot_exception_one['Bot Exception'].str.contains('reference mismatch', na=False)]),
    #                     "Balance greater than 1": len(bot_exception_one.loc[bot_exception_one['Bot Exception'].str.contains('balance greater than 1', na=False)]),
    #                     "Withholding Tax Tab": len(bot_exception_one.loc[bot_exception_one['Bot Exception'].str.contains('withholding tax tab', na=False)])}

    # three_month_exceptions = {"No GRN": len(bot_exception_three.loc[bot_exception_three['Bot Exception'].str.contains('no grn', na=False)]),
    #                     "Reference Mismatch": len(bot_exception_three.loc[bot_exception_three['Bot Exception'].str.contains('reference mismatch', na=False)]),
    #                     "Balance greater than 1": len(bot_exception_three.loc[bot_exception_three['Bot Exception'].str.contains('balance greater than 1', na=False)]),
    #                     "Withholding Tax Tab": len(bot_exception_three.loc[bot_exception_three['Bot Exception'].str.contains('withholding tax tab', na=False)])}

    # ! Change format of this!

    invoice_received = manual_inward + auto_inward
    pending_for_period = num_of_template_exceptions+ql_tl+num_of_invoice_verify
    cumulative_pending = num_of_template_exceptions_current+ql_tl_current+num_of_invoice_verify_current
    sent_to_manual = num_of_invoice_manual_ace+len(bot_exception)

    data = [
        ["Invoice Received", manual_inward if manual_inward else None, manual_inward if manual_inward else None, auto_inward if auto_inward else None, auto_inward if auto_inward else None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        ["Rejected", None, None, None, None, num_of_invoice_rejected if num_of_invoice_rejected else None, num_of_invoice_rejected if num_of_invoice_rejected else None, None, None, None, None, None, None, None,None,None,None,None,None,None,None, None,None,None,None],
        ["Pending for the Period", None, None, None, None,None,None,num_of_template_exceptions if num_of_template_exceptions else None, num_of_template_exceptions if num_of_template_exceptions else None, ql_tl if ql_tl else None,ql_tl if ql_tl else None, num_of_invoice_verify if num_of_invoice_verify else None,num_of_invoice_verify if num_of_invoice_verify else None,None,None,None,None,None,None,None,None,None,None,None,None],
        ["Cumulative Pending - Current", None, None, None, None,None,None,None,None,None,None,None,None,num_of_template_exceptions_current if num_of_template_exceptions_current else None, num_of_template_exceptions_current if num_of_template_exceptions_current else None, ql_tl_current if ql_tl_current else None,ql_tl_current if ql_tl_current else None, num_of_invoice_verify_current if num_of_invoice_verify_current else None,num_of_invoice_verify_current if num_of_invoice_verify_current else None,None,None,None,None,None,None],
        ["Sent to Manual",None, None, None, None,None,None,None,None,None,None,None,None,None, None, None,None,None,None,num_of_invoice_manual_ace if num_of_invoice_manual_ace else None,num_of_invoice_manual_ace if num_of_invoice_manual_ace else None,len(bot_exception) if len(bot_exception) else None,len(bot_exception) if len(bot_exception) else None,None,None],
        ["Processed", 0, None, 0, None,0,None,0,None,0,None,0,None,0, None, 0, None,0,None,0,None,0,None,len(bot_processed) if len(bot_processed) else 0,len(bot_processed) if len(bot_processed) else None]]
    column_names =  ['Process', 'Inwarded Manually', {'role': 'annotation'},'Inwarded by Bot', {'role': 'annotation'},'Rejected', {'role': 'annotation'}, 'New Template', {'role': 'annotation'},'QL + TL',{'role': 'annotation'}, 'Verification Pending', {'role': 'annotation'},'New Template', {'role': 'annotation'},'QL + TL',{'role': 'annotation'}, 'Verification Pending', {'role': 'annotation'},
    'ACE Deviation',{'role': 'annotation'}, 'Bot Deviation',{'role': 'annotation'}, 'By Bot',{'role': 'annotation'}]

#     nodes = [
#     {
#       "id": 'Invoices Ingested',
#       "label": 'Invoices Uploaded' + "," + str(num_of_invoice_uploaded),
#       "color": "#A7D588"
#     }, {
#         "id": '1',
#         "label": 'Pending' + "," + str(num_of_invoice_pending),
#         "color": "#EFC107"
#     }, {
#         "id": '2',
#         "label": 'SAP Inwarded' + "," + str(auto_inward+manual_inward),
#         "color": "#4ED9FF"
#     }, {
#         "id": '3',
#         "label": 'Rejected' + "," + str(num_of_invoice_rejected),
#         "color": "#FF5A5F"
#     }, {
#         "id": '4',
#         "label": 'Pending Allocation' + "," + str(num_of_invoice_uploaded - (len(bot_processed)+len(bot_exception)+num_of_invoice_manual_ace+num_of_invoice_pending+num_of_invoice_rejected)),
#         "color": "#EFC107"
#     }, {
#         "id": '5',
#         "label": 'BOT Processed' + "," + str(len(bot_processed)),
#         "color": "#3582FF"
#     }, {
#         "id": '6',
#         "label": 'Manual Queue' + "," + str(len(bot_exception) + num_of_invoice_manual_ace),
#         "color": "#A6A6A6"
#     }]

#     links = [{
#       "source": 'Invoices Ingested',
#       "target": '1'
#   }, {
#       "source": 'Invoices Ingested',
#       "target": '2'
#   }, {
#       "source": 'Invoices Ingested',
#       "target": '3'
#   }, {
#       "source": '2',
#       "target": '4'
#   }, {
#       "source": '2',
#       "target": '5'
#   }, {
#       "source": '2',
#       "target": '6'
#   }]

#     hover = {
#       1: {
#           "Verification Pending": num_of_invoice_verify,
#           "Quality Check + TL Verify": num_of_invoice_quality + num_of_invoice_tl,
#           "New Template": num_of_template_exceptions
#       },
#       2: {
#           "Auto Inward": auto_inward,
#           "Manual Inward": manual_inward,
#       },
#       6: {
#           "BOT Deviation": len(bot_exception),
#           "Manual Processing": num_of_invoice_manual_ace
#       }
#   }

    stats = [
        {
            'name': 'Invoices Pending',
            'value': num_of_invoice_pending,
            'icon': './assets/images/stats/doc.png'
        },
        {
            'name': 'Invoices Approved',
            'value': num_of_invoice_approved,
            'icon': './assets/images/stats/checked.svg'
        },
        {
            'name': 'Invoices Rejected',
            'value': num_of_invoice_rejected,
            'icon': './assets/images/stats/doc.png'
        },
        {
            'name': 'Invoice Processed (TL)',
            'value': ql_tl,
            'icon': './assets/images/stats/queue_1.png'
        },
        {
            'name': 'Successful SAP Inwards',
            'value': num_of_succssful_sap,
            'icon': './assets/images/stats/checked.svg'
        }
    ]

    charts = {
        "BOT vs Manual": [["Sent to Manual Queue", len(bot_exception)],["By BOT", len(bot_processed)]],
        "Exceptions Chart": [["No GRN", exceptions["No GRN"]],["Reference Mismatch", exceptions["Reference Mismatch"]],["Balance greater than 1", exceptions["Balance greater than 1"]],["Withholding Tax Tab", exceptions["Withholding Tax Tab"]]],
      }
    query = "SHOW COLUMNS FROM combined"
    df = extraction_db.execute_(query)
    total_fields = len(list(df['Field']))-5
    query = "SELECT `fields_changed` from `field_accuracy`"
    df = db.execute_(query)
    manual_changes = sum([len(json.loads(ele)) for ele in list(df['fields_changed'])])
    auto = len(df['fields_changed'])*total_fields

    print (f"MANUAL changes made are {manual_changes}, auto {auto}")

    query = "SELECT COUNT(*) FROM combined"
    df = extraction_db.execute_(query)

    print (f"COMBINEd {df['COUNT(*)'].iloc[0]}")
    if df['COUNT(*)'].iloc[0] == 0:
        pie_data = [['Auto', 0], ['No Data',auto]]
    else:
        pie_data = [['Manual', manual_changes], ['Auto',auto - manual_changes]]


    # data_empty = df.empty


    return jsonify({"charts": charts, "data": data, "column_names":column_names, "stats": stats, "piechart":pie_data})

@app.route('/get_dashboard_data', methods=['POST', 'GET'])
def get_dashboard_data():
    db_config = {
        'host': 'queue_db',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    db = DB('queues', **db_config)
    # db = DB('queues')

    process_queue = db.get_all('process_queue')

    # * Trace data. Trace of file from start till end.
    print('\nGetting trace data...')
    unique_cases = list(process_queue.case_id.unique())
    required_columns = ['case_id', 'queue', 'operator', 'created_date', 'time_spent', 'last_updated_by', 'last_updated']
    trace_data = {}
    for unique_case in unique_cases:
        case_trace = process_queue.loc[process_queue['case_id'] == unique_case]
        case_trace_filtered = case_trace[required_columns]
        trace_data[unique_case] = case_trace_filtered.to_dict(orient='records')
    # print(trace_data)

    # * All the queues
    print('\nGetting queues...')
    queue_definition = db.get_all('queue_definition')
    queues = list(queue_definition.name)
    # print(queues)

    # * Queue wise movement
    # How many invoices moved from one queue to another queue
    queue_movement = {}
    for _, case_flow in trace_data.items():
        for index, node in enumerate(case_flow):
            to_queue = node['queue']

            # 'Upload' is starting point
            if index == 0:
                if 'Upload' not in queue_movement:
                    queue_movement['Upload'] = {}

                if to_queue not in queue_movement['Upload']:
                    queue_movement['Upload'][to_queue] = 1
                else:
                    queue_movement['Upload'][to_queue] += 1
                from_queue = to_queue
                continue

            # Create 'from' key
            if from_queue not in queue_movement:
                queue_movement[from_queue] = {}

            # Create 'to' key
            if to_queue not in queue_movement[from_queue]:
                queue_movement[from_queue][to_queue] = {
                    'number_of_records': 0
                }

            queue_movement[from_queue][to_queue]['number_of_records'] += 1

            if node['last_updated_by'] is not None or not node['last_updated_by']:
                time_spent_by_user = {
                    node['last_updated_by']: node['time_spent'],
                }
                queue_movement[from_queue][to_queue]['time_spent_by_user'] = time_spent_by_user
            else:
                print(f'Last updated by field is None/empty. Skipping.')
            from_queue = to_queue

    dashboard_data = {
        'trace': trace_data,
        'queues': queues,
        'queue_movement': queue_movement
    }

    return jsonify({'flag': True, 'data': dashboard_data})


def update_table(db, case_id, file_name, changed_fields):

    query = f"SELECT `fields_changed` from `field_accuracy` WHERE case_id={case_id}"
    fields_json_string_df = db.execute_(query)
    if not fields_json_string_df.empty:
        fields_json_string = fields_json_string_df['fields_changed'][0]
        fields_json = json.loads(fields_json_string)
        total_fields = changed_fields.pop('total_fields', 1)
        print (f"fields json is ....previously...{fields_json}")
        fields_json.update(changed_fields)
        print (f"fields json is updated...{fields_json}")
        percentage = len(fields_json.keys())/total_fields
        # update the database record
        query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(fields_json)}', `percentage`= '{percentage}'  WHERE case_id={case_id}"
        db.execute(query)
        print ("UPDATED ROW")
    else:
        # new case_id that means insert record into the database
        total_fields = changed_fields.pop('total_fields', 1)
        percentage = len(changed_fields.keys())/total_fields
        print (f"NEW RECORD changed_fields are {changed_fields}")
        query = f"INSERT INTO `field_accuracy` (`id`, `case_id`, `file_name`, `fields_changed`, `percentage`) VALUES (NULL,'{case_id}','{file_name}','{json.dumps(changed_fields)}','{percentage}')"
        db.execute(query)
        print ("ADDED ROW")
    return "UPDATED TABLE"


@app.route('/save_changes', methods=['POST', 'GET'])
def save_changes():
    try:
        data = request.json
        case_id = data['case_id']
        fields = data['fields']
        changed_fields = data['field_changes']

        try:
            verify_operator = data['operator']
        except:
            print("operator key not found")
            verify_operator = None

        queue_db_config = {
            'host': 'queue_db',
            'port': 3306,
            'user': 'root',
            'password': 'root'
        }
        queue_db = DB('queues', **queue_db_config)
        try:
            update_table(queue_db, case_id, "", changed_fields)
        except:
            pass
        fields_def_df = queue_db.get_all('field_definition')
        tabs_def_df = queue_db.get_all('tab_definition')


        fields_w_name = {}
        for unique_name, value in fields.items():
            unique_field_def = fields_def_df.loc[fields_def_df['unique_name'] == unique_name]
            display_name = list(unique_field_def.display_name)[0]
            tab_id = list(unique_field_def.tab_id)[0]
            tab_info = tabs_def_df.ix[tab_id]
            table_name = tab_info.source

            if table_name not in fields_w_name:
                fields_w_name[table_name] = {}

            fields_w_name[table_name][display_name] = value

        extraction_db_config = {
            'host': 'extraction_db',
            'port': 3306,
            'user': 'root',
            'password': 'root'
        }
        extraction_db = DB('extraction', **extraction_db_config)

        for table, fields in fields_w_name.items():
            if table == 'ocr':
                fields['Invoice Date'] = fields['Invoice Date'][:10]
            if table == 'business_rule':
                fields['Verify Operator'] = verify_operator if verify_operator else ''
            extraction_db.update(table, update=fields, where={'case_id': case_id})

        return jsonify({'flag': True, 'message': 'Saved changes.'})
    except:
        traceback.print_exc()
        return jsonify({'flag': False, 'message': 'Something went wrong saving changes. Check logs.'})

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port number', default=5011)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')
    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, threaded=True, port=port, debug=False)
