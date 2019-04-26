from flask import Flask, jsonify, request, Response, send_file
import random
import string
import json
import logging
import pandas as pd
app = Flask(__name__)

first_name_set = ['Baker', 'Brant', 'Addison', 'Brice', 'Bliss', 'Astin', 'Cabal', 'Gala', 'Royce', 'Whitley', 'Rune', 'Yale', 'Zanda', 'Sanborn', 'Scarus',
                'Norwin', 'Orlan', 'Tacey', 'Talon', 'Taylor', 'Reed', 'Roan', 'Robson']
middle_name_set = []
for c in string.ascii_uppercase: middle_name_set.append(c + '.')
last_name_set = ['Smith', 'Jones', 'Tom', 'Mary', 'White', 'Harris', 'Haley', 'Cooper', 'Nelson', 'Carter', 'Wood', 'Ross', 'Barnes', 'Thomas', 'Annie',
                'Rose', 'Garcia', 'Grace', 'Maria', 'Robert', 'Rob', 'Hughes', 'Lopez', 'Hill', 'Kelly', 'Jane']
min_hour = int(365*38/7)
max_hour = int(365*50/7)
avg_hour = int(365*42/7)
min_salary = 20000
max_salary = 50000

@app.route('/')
def hello():
    return "Hello World!"

@app.route('/datagen/')
def generate_data():
    #return "Datagen not ready."
    logging.info('Generating test db')
    request_args = request.args.to_dict()
    if len(request_args) > 0:
        staff_db = gen_pd(**request_args)
    else:
        staff_db = gen_pd(50, 2009, 2018)
    staff_db.to_csv('test_db')
    return 'Successfully generated data'

@app.route('/reporting/')
def reporting():
    staff_db = pd.read_csv('test_db', index_col=0)
    request_args = request.args.to_dict()
    int_group = ['year_start', 'year_end', 'salary_min', 'salary_max', 'bonus_min', 'bonus_max']
    flt_group = ['working_hour_min', 'working_hour_max']
    # Convert value type from string to appropriate format
    for request_key in request_args:
        if request_key in int_group:
            request_args[request_key] = int(request_args[request_key])
        elif request_key in int_group:
            request_args[request_key] = float(request_args[request_key])
    print(request_args)
    reporting_db = test_report(staff_db=staff_db, **request_args)
    reporting_response = reporting_db.to_csv('reporting.csv')
    #reporting_json = reporting_db.to_json(orient='split')
    #reporting_json.replace('},', '},\n')
    #return reporting_response #Response(reporting_response, mime_type='text/html')
    return send_file('reporting.csv')

def gen_name(first_name_lib, middle_name_lib, last_name_lib):
    first_name = first_name_lib[round(random.random()*len(first_name_lib))-1]
    last_name = last_name_lib[round(random.random()*len(last_name_lib))-1]
    middle_name = middle_name_lib[round(random.random()*len(middle_name_lib))-1]
    if middle_name_lib != '':
        return (first_name + ' ' + middle_name + ' ' + last_name)
    else:
        return (first_name + ' ' + last_name)

def gen_score(min_scr, max_scr):
    return (min_scr + random.random()*(max_scr-min_scr))

def gen_id(id_list):
    out_id = '{0:15}'.format(random.randint(10**14, 10**15))
    if out_id in id_list:
        gen_id(id_list)
    else:
        return out_id

def gen_pd(num_staff, year_start, year_end):
    staff_list = []
    id_list = []
    board = {}
    board['staff_id'] = []
    board['staff_name'] = []
    board['working_hour'] = []
    board['base_salary'] = []
    board['bonus'] = []
    board['year'] = []
    for i in range(num_staff):
        staff_name = gen_name(first_name_set, middle_name_set, last_name_set)
        staff_id = gen_id(id_list)
        base_salary = gen_score(min_salary, max_salary)//1000*1000
        for year in range(year_start, year_end+1):
            working_hour = int(gen_score(min_hour, max_hour)*100)/100
            if working_hour>avg_hour:
                bonus = int((base_salary/avg_hour)*1.2*(working_hour-avg_hour))
            else:
                bonus = 0
            board['staff_id'].append(staff_id)
            board['staff_name'].append(staff_name)
            board['working_hour'].append(working_hour)
            board['base_salary'].append(base_salary)
            board['bonus'].append(bonus)
            board['year'].append(year)
    try:
        workhour_db = pd.DataFrame(board)
    except Exception as error:
        print(f'Unexpected error when generating database: {error}')
    finally:
        return workhour_db

def test_report(staff_db, group_by=None, aggregate='mean', year_start=None, year_end=None, salary_min=None, salary_max=None, working_hour_min=None, working_hour_max=None, bonus_min=None, bonus_max=None, staff_id=None):
    report_db = staff_db
    if year_start is not None:
        report_db = report_db.loc[report_db['year'] >= year_start]
    if year_end is not None:
        report_db = report_db.loc[report_db['year'] <= year_end]
    if salary_min is not None:
        report_db = report_db.loc[report_db['base_salary'] >= salary_min]
    if salary_max is not None:
        report_db = report_db.loc[report_db['base_salary'] <= salary_max]
    if working_hour_min is not None:
        report_db = report_db.loc[report_db['working_hour'] >= working_hour_min]
    if working_hour_max is not None:
        report_db = report_db.loc[report_db['working_hour'] <= working_hour_max]
    if bonus_min is not None:
        report_db = report_db.loc[report_db['bonus'] >= bonus_min]
    if bonus_max is not None:
        report_db = report_db.loc[report_db['bonus'] <= bonus_max]
    if staff_id is not None:
        report_db = report_db.loc[report_db['id'] == staff_id]
    if group_by is not None:
        groups = group_by.split(',')
        aggregates = aggregate.split(',')
        report_db = report_db.groupby(groups).agg(aggregates)[['working_hour', 'base_salary', 'bonus']]
    return report_db

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
    # Only 0.0.0.0 will work inside a container (not know of a way to make normal IP works yet)