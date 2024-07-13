## gcp 익히기

![img](https://github.com/dean-kg/data-engineering-study/blob/master/images/git_achieve_etl.png?raw=true)

### 1. gcloud SDK 세팅

### 2. bigquery 사전정의

```python
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
```

### 3. gcloud cloud function 배포

- main.py 파일내에 정의해야함
- return 값 정의해야함

```bash
gcloud functions deploy git_achieve_run --runtime python310 \
    --trigger-http \
    --allow-unauthenticated
```

### 4. gcloud scheduler 세팅

```bash
    --schedule "*/30 * * * *" \
    --http-method GET \
    --uri {cloud_functions URL} \
    --location  us-central1
```
