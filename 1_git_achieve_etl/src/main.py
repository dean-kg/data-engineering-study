import logging
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os


load_dotenv()

INFO_TABLE_NAME = os.getenv('INFO_TABLE_NAME')
META_TABLE_NAME = os.getenv('META_TABLE_NAME')
REPO_NAME =os.getenv('REPO_NAME')
REPO_URL =os.getenv('REPO_URL')
GITHUB_KEY =os.getenv('GITHUB_KEY')


'''

# BigQuery 테이블 스키마 정의
META_TABLE
schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("full_name", "STRING"),
    bigquery.SchemaField("html_url", "STRING"),
    bigquery.SchemaField("forks", "INTEGER"),
    bigquery.SchemaField("visibility", "STRING"),
    bigquery.SchemaField("size", "INTEGER"),
    bigquery.SchemaField("open_issues", "INTEGER"),
    bigquery.SchemaField("watchers", "INTEGER"),
    bigquery.SchemaField("network_count", "INTEGER"),
    bigquery.SchemaField("subscribers_count", "INTEGER"),
    bigquery.SchemaField("allow_forking", "BOOLEAN"),
    bigquery.SchemaField("created_at", "DATETIME"),
    bigquery.SchemaField("updated_at", "DATETIME"),
    bigquery.SchemaField("extract_at", "DATETIME"),
    bigquery.SchemaField("readme", "STRING"),
    bigquery.SchemaField("llm", "STRING"),
]

INFO_TABLE
schema = [
    bigquery.SchemaField("repo_id", "STRING"),
    bigquery.SchemaField("repo_name", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_count", "INTEGER"),
    bigquery.SchemaField("event_date", "DATETIME"),
]
'''





def get_client():    
    client = bigquery.Client()
    return client


def get_readme_text(url):
    res = requests.get(url)
    html = res.text
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find("article", attrs={"class":"markdown-body entry-content container-lg"})

    text = article.text.strip().replace('\n','') if article else ''

    return text

def _delete_data(client, target_date,date_column,TABLE_NAME):
    '''
    ref; C.B 
    '''
    target_date_str = target_date.strftime('%Y-%m-%d')

    query = f"""
    DELETE FROM {TABLE_NAME}
    WHERE DATE({date_column}) = {target_date_str}
    """
    query_job = client.query(query)
    logging.info(f"Existing data deleted")


    

def extract_github_data():
    # 쿼리 작성
    
    readme_url = REPO_URL
    url = f"https://api.github.com/repos/{REPO_NAME}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_KEY}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        target_col =['id','name','full_name','html_url','forks','visibility','size','open_issues','watchers'
                     ,'network_count','subscribers_count','allow_forking','created_at','updated_at']
        
        df_git = pd.DataFrame([data])
        df_git = df_git[[x for x in target_col if x in df_git.columns]]
        df_git['extract_at'] = pd.to_datetime(datetime.now())
        df_git['created_at'] = pd.to_datetime(df_git['created_at'])
        df_git['updated_at'] = pd.to_datetime(df_git['updated_at'])

        if readme_url:
            readme_text = get_readme_text(readme_url)
            llm = '' # 요약
        else:
            readme_text = None
            llm = '' # 요약


        df_git = df_git.assign(readme =readme_text)
        df_git = df_git.assign(llm =llm)

        return df_git

    else:
        logging.info(f"Error: {response.status_code}")
        return None # 에러 예외 코드 추가


def extract_data(client,target_date):
    # 쿼리 작성

    target_date_str = target_date.strftime('%Y%m%d')
    query = f"""
            SELECT *
            FROM (
                SELECT 
                    repo.id AS repo_id
                , repo.name AS repo_name
                , type AS event_type
                , COUNT(*) AS event_count
                FROM `githubarchive.day.{target_date_str}` 
                WHERE 
                    repo.name = 'vercel/next.js'
                GROUP BY 1, 2, 3 
                ORDER BY 1, 2, 3
            )
    """

    # 쿼리 실행 및 결과를 pandas DataFrame으로 읽기
    df = client.query(query).to_dataframe()

    return df
    
def load_data(client, df,table_name):
    client.load_table_from_dataframe(df, table_name)
    logging.info(f"Data loaded")


def git_achieve_run(event):
    try:

        client = get_client()
        target_date = datetime.today() - timedelta(days=1)

        # daily 데이터 추출 
        df = extract_data(client,target_date)
        df = df.assign(event_date = target_date.date())
        df['repo_id'] = df['repo_id'].astype('str')
        df['event_date'] = pd.to_datetime(df['event_date'])
        _delete_data(client,target_date,'event_date',INFO_TABLE_NAME)
        load_data(client, df,INFO_TABLE_NAME)


        # github repo에서 직접 데이터 추출
        df_git = extract_github_data()
        _delete_data(client,target_date,'extract_at',META_TABLE_NAME)
        load_data(client, df_git,META_TABLE_NAME)

        return {'status': 200}

    except:
        return {'status': 500}
