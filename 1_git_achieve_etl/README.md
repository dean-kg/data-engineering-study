## gcp 익히기

![img](https://github.com/dean-kg/data-engineering-study/blob/master/images/git_achieve_etl.png?raw=true)

### 1. gcloud SDK 세팅

### 2. gcloud cloud function 배포

```bash
gcloud functions deploy git_achieve_run --runtime python310 \
    --trigger-http \
    --allow-unauthenticated
```

### 3. gcloud scheduler 세팅

```bash
    --schedule "*/30 * * * *" \
    --http-method GET \
    --uri {cloud_functions URL} \
    --location  us-central1
```
