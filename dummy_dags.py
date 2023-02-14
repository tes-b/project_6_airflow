import requests, json, random, pymysql
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

def extract():
    hostname = 'root'
    username = 'root'
    userpassword = 'root'
    enc_type = 'utf8'

    conn_db = pymysql.connect(host=hostname,
                              user=username,
                              password=userpassword,
                              charset=enc_type)

    cur = conn_db.cursor()

    user_list_query = """
    select *
    from db
    where ~;
    """
    cur.execute(user_list_query)
    user_list = cur.fetchall()

    cur.close()
    conn_db.close()
    
    return user_list

data = extract()

def is_leapyear(year):
    if ((year % 4 == 0) and (year % 100 != 0)) or (year % 400 == 0):
        return True
    else:
        return False

def calculate_date():
    year = random.randint(1923,2022)
    month = random.randint(1,12)
    if month == 2:
        if is_leapyear(year):
            day = random.randint(1,29)
        else:
            day = random.randint(1,28)
    elif month in [1, 3, 5, 7, 8, 10, 12]:
        day = random.randint(1,31)
    else:
        day = random.randint(1, 30)
    
    if month < 10:
        month = str(month).zfill(2)

    if day < 10:
        day = str(day).zfill(2)

    return f"{year}-{month}-{day}"

def signin_api(username, password = "for_test"):
    url = "http://3.38.47.74/accounts/api/login/"
    
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    
    body = {
        "username": f"{username}",
        "password": f"{password}"        
    }
    response = requests.post(url, headers=headers, data = json.dumps(body, ensure_ascii = False, indent = "\t"))
    return response.json()

def for_dummy_question():
    """make dummy -> board"""
    for i in range(10):
        show = signin_api(data[i][4])
        
        url = "http://3.38.47.74/board/api/create/"
        
        headers = {'Content-Type' : 'application/json', 'charset': 'UTF-8', 'Accept': '*/*',
                    'Authorization' : f"Bearer {show['token']['access']}"}
        
        body = {
            'title' : f"dummy_test{i}",
            "content": f"{data[i][0]}",
            "author_id": f"{data[i][0]}",
        }

        response = requests.post(url, headers=headers, data = json.dumps(body, ensure_ascii = False, indent = "\t"))


def for_dummy_accounts(age, gender):
    """make dummy -> accounts"""
    rand_first_name = ['kim', 'cho', 'oh', 'gil', 'yi', 'ho', 'ha', 'he', 'ga', 'ja', 'rang']
    url = "http://3.38.47.74/accounts/api/signup/"
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    body = {
        "username": f"testNo{random.randrange(0,100000)}",
        "password": 'for_test',
        "password_check" : 'for_test',
        # "last_login" : f'{birth_date}', 보류
        "is_superuser" : 0,
        "first_name" : f'{random.choice(rand_first_name)}',
        "last_name" : f'dummy{random.randrange(0,100000)}',
        'email' : f"test@test{random.randrange(0,100000)}.com",
        "is_staff" : 0,
        "is_active" : 1,
        "age" : f"{age}",
        "gender" : f"{gender}"
    }
    
    try:
        response = requests.post(url, headers = headers, data = json.dumps(body, ensure_ascii = False, indent="\t"))
        print("response status %r" % response.status_code)
        print("response text %r" % response.text)
    except Exception as ex:
        print(ex)

def create_dummy_accounts():
    """create 10ea"""
    for _ in range(10):
        age = random.randint(1, 70)
        gender = random.choice(['Male', 'Female', 'Other'])
        for_dummy_accounts(age, gender)


default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 14),
}

dag = DAG('make_dummy_question, accounts',
          default_args=default_args,
          schedule_interval=timedelta(hours=1),
          catchup=False,  
          max_active_runs=1) 

t1 = PythonOperator(
    task_id='create_dummy_question',
    python_callable=for_dummy_question,
    dag=dag)

t2 = PythonOperator(
    task_id='create_dummy_accounts',
    python_callable=create_dummy_accounts,
    dag=dag)


t2 >> t1
