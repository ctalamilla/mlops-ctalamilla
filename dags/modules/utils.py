from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook("aws_conn")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)

def read_from_s3_json(key: str, bucket_name: str) -> None:
    hook = S3Hook("aws_conn")
    client = hook.get_conn()
    response = client.get_object(
        Bucket=bucket_name, Key=key
    )  # hook.read_key(key=key, bucket_name= bucket_name)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        file = response.get("Body")
        data = pd.read_json(file)
        print(data)
        return data