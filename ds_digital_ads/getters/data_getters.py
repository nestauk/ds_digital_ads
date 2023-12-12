"""
Data getters (and savers)
"""

import json
import io
import boto3
import os
from ds_digital_ads import PROJECT_DIR


def get_s3_resource():
    s3 = boto3.resource("s3")
    return s3


def save_to_s3(bucket_name, output_var, output_file_dir):
    s3 = get_s3_resource()

    obj = s3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + "/" + output_file_dir, index=False)
    elif fnmatch(output_file_dir, "*.parquet"):
        output_var.to_parquet(
            "s3://" + bucket_name + "/" + output_file_dir, index=False
        )
    elif fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        obj.put(Body=pickle.dumps(output_var))
    elif fnmatch(output_file_dir, "*.gz"):
        obj.put(Body=gzip.compress(json.dumps(output_var).encode()))
    elif fnmatch(output_file_dir, "*.txt"):
        obj.put(Body=output_var)
    else:
        obj.put(Body=json.dumps(output_var, cls=CustomJsonEncoder))

    logger.info(f"Saved to s3://{bucket_name} + {output_file_dir} ...")


def load_s3_data(bucket_name, file_name):
    """
    Load data from S3 location.

    bucket_name: The S3 bucket name
    file_name: S3 key to load
    """
    s3 = get_s3_resource()

    obj = s3.Object(bucket_name, file_name)
    if fnmatch(file_name, "*.jsonl.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return [json.loads(line) for line in file]
    if fnmatch(file_name, "*.yml") or fnmatch(file_name, "*.yaml"):
        file = obj.get()["Body"].read().decode()
        return yaml.safe_load(file)
    elif fnmatch(file_name, "*.jsonl"):
        file = obj.get()["Body"].read().decode()
        return [json.loads(line) for line in file]
    elif fnmatch(file_name, "*.json.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return json.load(file)
    elif fnmatch(file_name, "*.json"):
        file = obj.get()["Body"].read().decode()
        return json.loads(file)
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.parquet"):
        return pd.read_parquet("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.pkl") or fnmatch(file_name, "*.pickle"):
        file = obj.get()["Body"].read().decode()
        return pickle.loads(file)
    else:
        logger.error(
            'Function not supported for file type other than "*.csv", "*.parquet", "*.jsonl.gz", "*.jsonl", or "*.json"'
        )


def dictionary_to_s3(data_dict: dict, s3_bucket: str, s3_folder: str, file_name: str):
    """
    Transforms a dictionary into a json and uploads to S3.
    Args:
        data_dict: dictionary with the data
        s3_bucket: S3 bucket name where to upload the file
        s3_folder: folder where to store the file within the S3 bucket
        file_name: name of the file
    """
    s3_client = boto3.client("s3")
    obj = io.BytesIO(json.dumps(data_dict).encode("utf-8"))
    s3_client.upload_fileobj(obj, s3_bucket, os.path.join(s3_folder, file_name))


def save_json_to_local_inputs_folder(data_dict: dict, folder: str, file_name: str):
    """
    Saves json file to local inputs folder.

    Args:
        data_dict: dictionary with the data
        folder: path to local folder, within the inputs/ folder, where to store data
        file_name: name of the file
    """

    path_local_inputs = os.path.join(PROJECT_DIR, "inputs/")
    full_path = os.path.join(path_local_inputs, folder)

    # Creates local folder path if it does not exist
    os.makedirs(full_path, exist_ok=True)

    obj = json.dumps(data_dict)
    # Writing to sample.json
    with open(os.path.join(full_path, file_name), "w") as outfile:
        outfile.write(obj)


def read_json_from_s3(bucket: str, file_path: str) -> dict:
    """
    Reads a json file from S3 without downloading it.

    Args:
        bucket: S3 bucket name
        file_path: file path (including file name)
    Returns:
        dictionary with json file data
    """
    s3_resource = boto3.resource("s3")
    json_file = s3_resource.Object(bucket, file_path)
    json_file = json_file.get()["Body"].read().decode("utf-8")
    return json.loads(json_file)


def read_json_from_local_path(file_path: str) -> dict:
    """
    Reads a json file from a local folder.

    Args:
        file_path: file path (including file name)
    Returns:
        dictionary with json file data
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    return data
