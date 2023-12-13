from botocore.exceptions import ClientError
from ..generic.config import load_item_from_config_file
import boto3
from datetime import datetime, timedelta
from io import BytesIO
import zipfile
from dateutil.relativedelta import relativedelta




def get_secret_as_dict(secret_id, base64_encoded=False):
    """Retrieve a secret from AWS Secrets Manager by the secret_id.
    :param str secret_id: SecretId of the secret within AWS Secrets Manager
    :param bool base64_encoded: Boolean value indicating if the secret is base64 encoded.
    Default: False
    :return dict/str: Secret string retrieved from AWS Secrets Manager. Can be either string or JSON"""
    
    secrets_client = boto3.client("secretsmanager")
    secret_string = secrets_client.get_secret_value(SecretId=secret_id).get("SecretString")

    if not base64_encoded:
        secret_dict = eval(secret_string)
    else:
        secret_dict = base64.b64decode(secret_string).decode("UTF-8")
    return secret_dict

def get_latest_partition_folder_name(s3_resource, bucket_name, table_name):
    """
    Get the folder name of the latest partition to be used for predicate pushdown in data loading.
    :param object s3_resource: Boto3 s3_resource object
    :param str bucket_name: Name of the bucket
    :param str table_name: Name of the table
    :return: str: Folder name containing relevant metadata of the table schema
    """

    # Get the current datetime
    current_datetime = datetime.today()

    # Get the bucket object
    bucket_object = s3_resource.Bucket(bucket_name)

    # Set initial parameters
    iteration = 1
    found_partition = False

    # Use a while loop to find a file for a certain month
    while not found_partition:

        year = str(current_datetime.year)
        month = str(current_datetime.month)

        # Get all S3 objects that adhere to the filter
        objects = list(bucket_object.objects.filter(Prefix=f"{table_name}/job_start_time={year}-{month}"))

        # Check if we found a file for this filter
        if len(objects) > 1:
            found_partition = True

        # Throw an error after 12 months
        elif iteration > 12:
            raise Exception("No partition found in the last 12 months")

        # If not, subtract a month and try again
        else:
            current_datetime = current_datetime - relativedelta(months=1)
            iteration += 1

    # Sort the objects and select the relevant part of the latest one
    objects.sort(key=lambda o: o.last_modified)
    job_start_time_string = objects[-1].key.split('/')[2]
    return job_start_time_string


def delete_file_from_s3(s3_resource, bucket, prefix, log, fname=None):
    """
    Remove file with name fname from bucket in s3 using a prefix
    :param boto3.resources.factory.s3.ServiceResource s3_resource: s3 resoruce used to delete file
    :param str bucket: Name of s3 bucket
    :param str prefix: Prefix to use
    :param str fname: Name of file to delete from s3
    :param logger log: Initialized logger to be used for logging
    """
    # Try to delete the file if present. Otherwise log is was not found:
    if fname:
        path_to_delete = f"{prefix}/{fname}"
    else:
        path_to_delete = f"{prefix}/"
    try:
        bucket = s3_resource.Bucket(bucket)
        bucket.objects.filter(Prefix=path_to_delete).delete()
        log.info(f"Successfully deleted file: {path_to_delete} from s3")
    except ClientError:
        log.info(f"File: {path_to_delete} not found in s3")


def unzip_file_from_s3(s3, bucket_name, location_table, table, file_name, last_modified, log):
    """
    Unzip file in S3 bucket and upload it to the same S3 bucket
    :param boto3.resources.factory.s3.ServiceResource s3: s3 resource used to get the file
    :param str bucket_name: Name of s3 bucket
    :param str location_table: Folder in which the table should be copied 
    :param str table: Name of csv table
    :param str file_name: Name of zip file in s3
    :param str last_modified: Last modified date of file
    """
    # Get the zip file
    zip_file = s3.Object(bucket_name=bucket_name, key=f"{location_table}/{table}/{file_name}")
    # Read the zip file
    buffer = BytesIO(zip_file.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    # Copy the unzipped table to s3 bucket, with last modified date as metadata
    for filename in z.namelist():
        # file_info = z.getinfo(filename)
        s3.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket_name,
            Key=f"{location_table}/{table}/{table}",
            ExtraArgs={"Metadata": {"LastModifiedTime": str(last_modified)}},
        )
    # Delete the zip file
    delete_file_from_s3(s3, bucket_name, f"{location_table}/{table}", log, file_name)


def copy_file(bucket_name_from, bucket_name_to, table_name, last_modified_date, location_table):
    """
    Copy file from a S3 bucket to another

    Parameters
    ----------
    bucket_name_from : Source bucket
    bucket_name_to : Destination bucket
    table_name : file name
    last_modified_date: date in which object was last modified
    """

    s3 = boto3.client("s3")
    copy_source = {"Bucket": f"{bucket_name_from}", "Key": f"{table_name}"}
    copy_target = f"{location_table}/{table_name}/{table_name}"

    last_modified_str = str(last_modified_date)
    last_modified = datetime.strptime(last_modified_str[:-6], "%Y-%m-%d %H:%M:%S").strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    s3.copy_object(
        Bucket=bucket_name_to,
        CopySource=copy_source,
        Key=copy_target,
        Metadata={"LastModifiedTime": last_modified},
        MetadataDirective="REPLACE",
    )


def copy_file_csv(
    bucket_name_from,
    bucket_name_to,
    table_name,
    prefix_to,
    synced_workflow,
    region,
    prefix_table_to=None,
):
    """
    Copy file from a S3 bucket to another

    Parameters
    ----------
    bucket_name_from : Source bucket
    bucket_name_to : Destination bucket
    file_name : file name
    prefix_to: prefix to use when copying the file
    prefix_table_to: prefix to use in the table name in target bucket
    """
    # Load name of source file in Landing zone from config. The data in de Landing zone has a prefix
    # with equal name as the source_key to make the data queriable with Athena. Hence, the
    # prefix_from is equal to the source_key:

    if synced_workflow:
        mapped_table_name = load_item_from_config_file(
            "configurations.config", "mapping_tables", table_name
        )
        source_key = f"{table_name[:-4]}_{region}.csv"
        prefix_from = f"{region}/{mapped_table_name}"

    else:
        source_key = load_item_from_config_file("configurations.config", "source_files", table_name)
        prefix_from = f"{region}/{source_key}"

    s3 = boto3.resource("s3")
    copy_source = {"Bucket": f"{bucket_name_from}", "Key": f"{prefix_from}/{source_key}"}

    bucket = s3.Bucket(bucket_name_to)

    if prefix_table_to:
        copy_target = f"{prefix_to}/{prefix_table_to}_{table_name[:-4]}.csv/{table_name[:-4]}"
    else:
        copy_target = f"{prefix_to}/{table_name[:-4]}.csv/{table_name[:-4]}"

    bucket.copy(copy_source, f"{copy_target}_{region}.csv")


def copy_file_parquet(
    bucket_name_from, bucket_name_to, table_name, prefix_to, region, prefix_table_to=None
):
    """
    Copy file from a S3 bucket to another

    Parameters
    ----------
    bucket_name_from : Source bucket
    bucket_name_to : Destination bucket
    file_name : file name
    prefix_to: prefix to use when copying the file
    prefix_table_to: prefix to use in the table name in target bucket
    """
    # Load name of source file in Landing zone from config. The data in de Landing zone has a prefix
    # with equal name as the source_key to make the data queriable with Athena. Hence, the
    # prefix_from is equal to the source_key:
    source_key = load_item_from_config_file("configurations.config", "source_files", table_name)
    prefix_from = f"{region}/{source_key}"

    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    more_objects = True
    found_token = True
    while more_objects:
        if found_token:
            response = s3_client.list_objects_v2(
                Bucket=f"{bucket_name_from}", Prefix=f"{prefix_from}/", Delimiter="/"
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=f"{bucket_name_from}",
                ContinuationToken=found_token,
                Prefix=f"{prefix_from}/",
                Delimiter="/",
            )

        for source in response["Contents"]:
            partition_name = source["Key"].split("/")[-1]

            copy_source = {
                "Bucket": f"{bucket_name_from}",
                "Key": f"{prefix_from}/{partition_name}",
            }
            bucket = s3_resource.Bucket(bucket_name_to)

            if prefix_table_to:
                copy_target = (
                    f"{prefix_to}/{prefix_table_to}_{table_name[:-8]}.parquet/{partition_name[:-8]}"
                )
            else:
                copy_target = f"{prefix_to}/{table_name[:-8]}.parquet/{partition_name[:-8]}"

            bucket.copy(copy_source, f"{copy_target}_{region}.parquet")

            # Now check there are more objects to list
            if "NextContinuationToken" in response:
                found_token = response["NextContinuationToken"]
                more_objects = True
            else:
                more_objects = False


def update_last_modified_date_parquet(bucket_name, prefix, region, last_modified):
    """
    Update the last modified date of all parquet partitions by copying the file in S3

    Parameters
    ----------
    bucket_name : bucket
    prefix : file name
    location_table: prefix to use when copying the file
    region: region of file. 
    """
    prefix_from = f"{region}/{prefix}"

    s3 = boto3.client("s3")
    more_objects = True
    found_token = True
    while more_objects:
        if found_token:
            response = s3.list_objects_v2(
                Bucket=f"{bucket_name}", Prefix=f"{prefix_from}/", Delimiter="/"
            )
        else:
            response = s3.list_objects_v2(
                Bucket=f"{bucket_name}",
                ContinuationToken=found_token,
                Prefix=f"{prefix_from}/",
                Delimiter="/",
            )

        for source in response["Contents"]:
            partition_name = source["Key"].split("/")[-1]

            # Target and source are identical, we only update the Metadata
            copy_source = {"Bucket": f"{bucket_name}", "Key": f"{prefix_from}/{partition_name}"}
            copy_target = f"{prefix_from}/{partition_name}"

            s3.copy_object(
                Bucket=bucket_name,
                CopySource=copy_source,
                Key=copy_target,
                Metadata={"LastModifiedTime": last_modified},
                MetadataDirective="REPLACE",
            )

            # Now check there are more objects to list
            if "NextContinuationToken" in response:
                found_token = response["NextContinuationToken"]
                more_objects = True
            else:
                more_objects = False


def create_file_in_s3(s3_client, bucket, prefix, fname):
    """
    Create empty file with name fname in bucket in s3 using a prefix.
    :param botocore.client.S3 s3_client: s3 client used when loading file to s3
    :param str bucket: Name of s3 bucket
    :param str prefix: Prefix to use
    :param str fname: Name of file to create in s3
    """
    s3_client.put_object(Bucket=bucket, Key=f"{prefix}/{fname}")


def compare_modified_date_source_with_s3(
    s3_client, bucket_name, table, last_modified_source, location_table, source, log
):
    """Retrieve modified date of the table in S3 and compare it to modified date
    of the same table in the source . If they do not match, append it to the list
    of different modified dates
    :param botocore.client.S3 s3_client : Botocore S3 client to communicate with S3
    :param str bucket_name : Name of the S3 bucket in which the file is located
    :param str table : Name of the table that should be compared
    :param datetime last_modified_source : last modified date of the file in the source
    (sftp, s3, dashDB)
    :param str location_table : name of the directory in which the table is located
    (APAC, EMEA, GLOBAL)
    :param str source: source of the file 
    :param logging.Logger log: logger to use to log the messages
    :return bool : True if modified dates are the same, else False
    """
    today = datetime.now()
    tnow_minus_24h = today - timedelta(hours=24)

    try:
        # Get the file name corresponding to the currently evaluated table
        # For most files file_name is equal to the table name, but not for GBW_US_ORDERS
        # because it is a parquet file
        response = s3_client.list_objects_v2(
            Bucket=f"{bucket_name}", Prefix=f"{location_table}/{table}/", Delimiter="/"
        )
        file_name = response["Contents"][0]["Key"].split("/")[-1]

        response_s3 = s3_client.head_object(
            Bucket=bucket_name, Key=f"{location_table}/{table}/{file_name}"
        )
        last_modified_s3_str = response_s3["Metadata"]["lastmodifiedtime"]
        last_modified_s3 = datetime.strptime(last_modified_s3_str, "%Y-%m-%d %H:%M:%S")
        log.info(
            f"Modify times {table}: {source} - {last_modified_source}, S3 - {last_modified_s3}"
        )

        if last_modified_source == last_modified_s3:
            log.info(f"Table {table} is up to date in S3")
            return True
        elif last_modified_source > tnow_minus_24h and last_modified_source < today:
            log.info(f"Table {table} needs to be updated in S3")
            return False
        else:
            log.info(f"Table {table} is outdated in S3 but waiting today's update")
            return True
    except KeyError:
        log.info(f"Modify times {table}: {source} - {last_modified_source}")
        if last_modified_source > tnow_minus_24h and last_modified_source < today:
            log.info(
                f"Table {table} not located in the S3 bucket. Landing job needs to be started."
            )
            return False
        else:
            log.info(f"Table {table} not located in the S3 bucket. Waiting for today's update.")
            return True
