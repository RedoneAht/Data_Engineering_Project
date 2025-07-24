from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
import json
import requests

default_args = {
    'owner': 'redone',
    'start_date': datetime(2025, 7, 23),

}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['email'] = res['email']
    data['gender'] = res['gender']
    data['address'] = str(res['location']['street']['number']) + ' ' + res['location']['street']['name'] + ' ' + res['location']['city'] + ' ' + res['location']['state'] + ' ' + res['location']['country']
    data['postcode'] = res['location']['postcode']
    data['phone'] = res['phone']
    data['dob'] = res['dob']['date']
    data['username'] = res['login']['username']
    data['regestered_date'] = res['registered']['date']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))
    
    
    

# with DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:
#     streaming_task = PythonOperator(
#         task_id='streaming_task',
#         python_callable=stream_data,
#     )

    
stream_data()