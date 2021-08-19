import sys
import logging
import json
import time
import os
import hashlib
from random import choice
from string import ascii_letters, digits
from urllib.parse import urlparse

from gen3.samples.azure_blob_storage_client import AzureBlobStorageClient

from gen3.index import Gen3Index
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File


logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# please see example .env.template file.

COMMONS_URL = os.environ.get("COMMONS_URL", "https://mycommons.azurefd.net")
PROGRAM_NAME = os.environ.get("PROGRAM_NAME", "Program1")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "project1")
PROJECT_CODE = os.environ.get("PROJECT_CODE", "P1")
CREDENTIALS_FILE_PATH = os.environ.get("CREDENTIALS_FILE_PATH", "credentials.json")

INDEXD_AUTHZ = os.environ.get("COMMONS_AUTHZ", "/programs")
INDEXD_ACL = os.environ.get("COMMONS_ACL", "*")

DATA_TYPE = "image"


def get_file_size(file_path):
    """
    Get the file size for a given file_path
    This assumes that the file_path is reachable from the executing context

    :param str file_name:
        file_name to use for getting the size
    """
    return os.path.getsize(file_path)


def calculate_md5(file_path):
    """
    Calculate the md5 for a given file_path
    This assumes that the file_path is reachable from the executing context

    :param str file_name:
        file_name to use for getting the md5
    """
    with open(file_path, "rb") as f:
        file_bytes = f.read()
        readable_hash = hashlib.md5(file_bytes).hexdigest()
        return readable_hash


def upload_file_azure(
    file_md5,
    file_size,
    file_name,
    url,
    authz=INDEXD_AUTHZ,
    acl=INDEXD_ACL,
    expires_in=200,
):
    """
    Index a file using Azure Blob Storage
    Get a SAS token from Fence to upload
    Use the SAS token with Azure Blob Storage to load the file
    to the new location (e.g. container_name/blob_name/file_name
    where blob_name can have a /guid in it)

    :param str file_md5:
        file_md5 to use for upload
        For example, you can use calculate_md5(file_name)
    :param int file_size:
        file_size in bytes for the given file to use for upload
        For example, you can use get_file_size(file_name)
    :param str file_name:
        file_name to use for upload
    :param str url:
        URL where the file is hosted in Azure Blob Storage.
        Note, you'll want to replace the protocol with `az`.
        For example, suppose the url for the existing file is:
        https://storageaccount.blob.core.windows.net/container/folder/test_file.txt
        You will want to instead use the following value:
        az://storageaccount.blob.core.windows.net/container/folder/test_file.txt
    :param str authz:
        authorization scope for the file, optional.
        Defaults to INDEXD_AUTHZ (e.g. `/programs`)
    :param str acl:
        Access control scope for the file, optional.
        Defaults to INDEXD_ACL (e.g. `'*'`)
    :param int expires_in:
        Amount in seconds that the signed url will expire from datetime.utcnow().
        Be sure to use a positive integer.
        This value will also be treated as <= MAX_PRESIGNED_URL_TTL in the fence configuration.
    """
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    index = Gen3Index(auth.endpoint, auth_provider=auth)
    response = index.create_record(
        hashes={"md5": file_md5},
        file_name=file_name,
        size=file_size,
        acl=[acl],
        urls=[url],
        authz=[authz],
    )

    document_id = response["did"]

    print(f"added index for file got document_id {document_id} and response {response}")

    gen3_file = Gen3File(endpoint=auth.endpoint, auth_provider=auth)
    upload_response = gen3_file.upload_file(
        file_name=file_name, authz=None, protocol="az", expires_in=expires_in
    )

    print(f"requested an upload SAS and got upload_response {upload_response}")

    parsed_url = urlparse(upload_response["url"])
    account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    container_and_blob_parts = parsed_url.path.strip("/").split("/")

    container_name = container_and_blob_parts[0]
    blob_name = "/".join(container_and_blob_parts[1:])

    print(
        f"testing upload using account_url {account_url} and credential {parsed_url.query}"
    )

    # upload the file using the SAS
    azure_blob_storage_client = AzureBlobStorageClient(
        account_url=account_url, credential=parsed_url.query
    )
    azure_blob_storage_client.upload_file_content_to_blob(
        container_name=container_name, blob_name=blob_name, source_file_path=file_name
    )

    print(
        f"finished uploading to container_name {container_name} blob_name {blob_name} with file_name {file_name}"
    )


def submit_metadata_sheepdog(file_name, file_md5, file_size, object_id):
    """
    Create a placeholder core metadata collection and a placeholder slide image metadata
    Submit the metadata to sheepdog

    :param str file_name:
        file_name to use as the data file
    :param str file_md5:
        md5sum for the given file
    :param int file_size:
        file size in bytes for the given file
    :param str object_id:
        This is the indexed documentid (`did`) / baseid for the already indexed file
    """
    submitter_id = _simple_generate_string_data(10)

    # for simplicity, create a core metadata collection per slide image
    # 1:1 instead of 1:n relationship
    coremetadata_json = _create_coremetadata_collection(
        project_code=PROJECT_CODE,
        submitter_id=submitter_id,
    )

    json_result = _submit_node_to_graph(coremetadata_json)

    print(f"submit_metadata_sheepdog got json_result {json_result}")

    core_metadata_node_id = json_result["entities"][0]["id"]

    _, file_extension = os.path.splitext(file_name)
    file_extension = file_extension.replace(".", "")

    slide_image_json = _create_slide_image_json(
        core_metadata_node_id=core_metadata_node_id,
        submitter_id=submitter_id,
        file_name=file_name,
        md5_value=file_md5,
        file_size=file_size,
        data_format=file_extension,
        data_type=DATA_TYPE,
        object_id=object_id,
    )

    slide_image_json_result = _submit_node_to_graph(slide_image_json)

    print(f"got slide_image_json_result {slide_image_json_result}")


def add_index_and_metadata_azure(
    file_md5, file_size, file_name, url, authz=INDEXD_AUTHZ, acl=INDEXD_ACL
):
    """
    Index a file using Azure Blob Storage
    Submit placeholder metadata to Sheepdog
    Get a presigned URL from Fence using the `az` protocol

    :param str file_md5:
        file_md5 to use for upload
        For example, you can use calculate_md5(file_name)
    :param int file_size:
        file_size in bytes for the given file to use for upload
        For example, you can use get_file_size(file_name)
    :param str file_name:
        file_name to use for upload
    :param str url:
        URL where the file is hosted in Azure Blob Storage.
        Note, you'll want to replace the protocol with `az`.
        For example, suppose the url for the existing file is:
        https://storageaccount.blob.core.windows.net/container/folder/test_file.txt
        You will want to instead use the following value:
        az://storageaccount.blob.core.windows.net/container/folder/test_file.txt
    :param str authz:
        authorization scope for the file, optional.
        Defaults to INDEXD_AUTHZ (e.g. `/programs`)
        If you want to use public authz, set this to `None`.
    :param str acl:
        Access control scope for the file, optional.
        Defaults to INDEXD_ACL (e.g. `'*'`)
    """
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    index = Gen3Index(auth.endpoint, auth_provider=auth)

    if not index.is_healthy():
        print("indexd service is unhealthy")
        return

    try:
        if authz is None:
            response = index.create_record(
                hashes={"md5": file_md5},
                file_name=file_name,
                size=file_size,
                acl=[acl],
                urls=[url],
            )
        else:
            response = index.create_record(
                hashes={"md5": file_md5},
                file_name=file_name,
                size=file_size,
                acl=[acl],
                urls=[url],
                authz=[authz],
            )

        document_id = response["did"]

        submit_metadata_sheepdog(file_name, file_md5, file_size, document_id)

        # should be able to get a presigned url
        presigned_url = get_presigned_url(document_id, "az")

        print(f"got presigned_url {presigned_url}")
    except Exception:
        print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
        )
    return response


def add_index_and_metadata_azure_delete_file(
    file_md5, file_size, file_name, url, authz=INDEXD_AUTHZ, acl=INDEXD_ACL
):
    """
    Index a file using Azure Blob Storage
    Submit placeholder metadata to Sheepdog
    Delete the file (from `indexd` and `fence`, and from Azure Blob Storage)

    If successful you should expect a response like ('', 204)

    :param str file_md5:
        file_md5 to use for upload
        For example, you can use calculate_md5(file_name)
    :param int file_size:
        file_size in bytes for the given file to use for upload
        For example, you can use get_file_size(file_name)
    :param str file_name:
        file_name to use for upload
    :param str url:
        URL where the file is hosted in Azure Blob Storage.
        Note, you'll want to replace the protocol with `az`.
        For example, suppose the url for the existing file is:
        https://storageaccount.blob.core.windows.net/container/folder/test_file.txt
        You will want to instead use the following value:
        az://storageaccount.blob.core.windows.net/container/folder/test_file.txt
    :param str authz:
        authorization scope for the file, optional.
        Defaults to INDEXD_AUTHZ (e.g. `/programs`)
    :param str acl:
        Access control scope for the file, optional.
        Defaults to INDEXD_ACL (e.g. `'*'`)
    """
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    index = Gen3Index(auth.endpoint, auth_provider=auth)

    if not index.is_healthy():
        print("indexd service is unhealthy")
        return

    try:
        response = index.create_record(
            hashes={"md5": file_md5},
            file_name=file_name,
            size=file_size,
            acl=[acl],
            urls=[url],
            authz=[authz],
        )

        document_id = response["did"]

        submit_metadata_sheepdog(file_name, file_md5, file_size, document_id)

        # should be able to get a presigned url
        presigned_url = get_presigned_url(document_id, "az")

        print(f"got presigned_url {presigned_url}")
        gen3_file = Gen3File(endpoint=auth.endpoint, auth_provider=auth)

        # attempt to delete file
        print(f"attempting to delete the file with document_id {document_id}")
        response = gen3_file.delete_files(guid=document_id)

        print(f"got response {response}")
    except Exception:
        print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
        )
    return response


def create_and_delete_index_azure(
    file_md5,
    file_size,
    file_name,
    url,
    authz=INDEXD_AUTHZ,
    acl=INDEXD_ACL,
):
    """
    Index a file using Azure Blob Storage
    Delete the file (from `indexd` and `fence`, and from Azure Blob Storage)

    If successful you should expect a response like ('', 204)

    :param str file_md5:
        file_md5 to use for upload
        For example, you can use calculate_md5(file_name)
    :param int file_size:
        file_size in bytes for the given file to use for upload
        For example, you can use get_file_size(file_name)
    :param str file_name:
        file_name to use for upload
    :param str url:
        URL where the file is hosted in Azure Blob Storage.
        Note, you'll want to replace the protocol with `az`.
        For example, suppose the url for the existing file is:
        https://storageaccount.blob.core.windows.net/container/folder/test_file.txt
        You will want to instead use the following value:
        az://storageaccount.blob.core.windows.net/container/folder/test_file.txt
    :param str authz:
        authorization scope for the file, optional.
        Defaults to INDEXD_AUTHZ (e.g. `/programs`)
    :param str acl:
        Access control scope for the file, optional.
        Defaults to INDEXD_ACL (e.g. `'*'`)
    """
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    index = Gen3Index(auth.endpoint, auth_provider=auth)

    if not index.is_healthy():
        print("indexd service is unhealthy")
        return

    try:
        response = index.create_record(
            hashes={"md5": file_md5},
            file_name=file_name,
            size=file_size,
            acl=[acl],
            urls=[url],
            authz=[authz],
        )

        document_id = response["did"]

        gen3_file = Gen3File(endpoint=auth.endpoint, auth_provider=auth)

        response = gen3_file.delete_files(guid=document_id)
    except Exception:
        print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
        )
    return response


def create_index_azure(
    file_md5,
    file_size,
    file_name,
    url,
    authz=INDEXD_AUTHZ,
    acl=INDEXD_ACL,
):
    """
    Index a file using Azure Blob Storage

    :param str file_md5:
        file_md5 to use for upload
        For example, you can use calculate_md5(file_name)
    :param int file_size:
        file_size in bytes for the given file to use for upload
        For example, you can use get_file_size(file_name)
    :param str file_name:
        file_name to use for upload
    :param str url:
        URL where the file is hosted in Azure Blob Storage.
        Note, you'll want to replace the protocol with `az`.
        For example, suppose the url for the existing file is:
        https://storageaccount.blob.core.windows.net/container/folder/test_file.txt
        You will want to instead use the following value:
        az://storageaccount.blob.core.windows.net/container/folder/test_file.txt
    :param str authz:
        authorization scope for the file, optional.
        Defaults to INDEXD_AUTHZ (e.g. `/programs`)
    :param str acl:
        Access control scope for the file, optional.
        Defaults to INDEXD_ACL (e.g. `'*'`)
    """

    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    index = Gen3Index(auth.endpoint, auth_provider=auth)
    if not index.is_healthy():
        print("indexd service is unhealthy")
        return

    try:
        response = index.create_record(
            hashes={"md5": file_md5},
            file_name=file_name,
            size=file_size,
            acl=[acl],
            urls=[url],
            authz=[authz],
        )
    except Exception:
        print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
        )
    return response


def _create_coremetadata_collection(submitter_id, project_code=PROJECT_NAME):
    return {
        "projects": {"code": project_code},
        "type": "core_metadata_collection",
        "submitter_id": submitter_id,
    }


def _create_slide_image_json(
    core_metadata_node_id,
    submitter_id,
    file_name,
    md5_value,
    file_size,
    data_format,
    data_type,
    object_id,
):
    return {
        "data_category": "Slide Image",
        "md5sum": md5_value,
        "core_metadata_collections": {
            "node_id": core_metadata_node_id,  # this should match the corresponding coremetadatacollection node_id
            "submitter_id": submitter_id,
        },
        "submitter_id": submitter_id,
        "type": "slide_image",
        "file_name": file_name,
        "data_format": data_format,
        "file_size": file_size,
        "data_type": data_type,
        "object_id": object_id,
    }


def _simple_generate_string_data(size=10):
    return "".join(choice(ascii_letters + digits) for x in range(size))


def _submit_node_to_graph(gen3_node_json):
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    sheepdog_client = Gen3Submission(COMMONS_URL, auth)

    logging.info(f"Created json file:\n{json.dumps(gen3_node_json, indent=2)}\n")
    json_result = sheepdog_client.submit_record(
        PROGRAM_NAME, PROJECT_NAME, gen3_node_json
    )
    logging.info(f"\n\njson result:\n\n{json_result}")

    return json_result


def get_presigned_url(node_guid, file_protocol):
    """
    Get a presigned url for an indexed file

    :param str node_guid:
        This is either the baseid or dic (documentid)
        For the indexed file
    :param str file_protocol:
        Get a presigned URL for a given protocol.
        For example, you can use `az` if the file is stored
        in Azure Blob Storage
    """
    auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
    fence_client = Gen3File(endpoint=COMMONS_URL, auth_provider=auth)
    json_result = fence_client.get_presigned_url(node_guid, protocol=file_protocol)

    presigned_url = f"{COMMONS_URL}/files/{node_guid}"
    logging.info(f"\n\njson result:\n\n{json_result}\n")
    logging.info(
        f"\n\nPlease visit this address in your browser:\n\n{COMMONS_URL}/files/{node_guid}\n"
    )

    return presigned_url


def get_blobs_metadata():
    """
    Get Blobs based on .env settings from Azure Blob Storage

    This will try to pull files that exist in a given folder
    e.g. set STARTS_WITH_FILTER to folder_1/ to get blobs that are in folder_1

    You should get a list of blob metadata which includes metadata rows like
    md5, filename, urls, size, and other fields used for indexing the file
    """
    azure_blob_storage_client = AzureBlobStorageClient()

    blobs = azure_blob_storage_client.get_blobs()
    blobs_metadata = azure_blob_storage_client.prepare_blob_index_metadata(
        blobs=blobs, desired_scheme="az"
    )

    for blob_metadata in blobs_metadata:
        blob_file_name = blob_metadata["filename"]
        file_name = os.path.basename(blob_file_name)
        # confirm the file name used
        print(file_name)

    return blobs_metadata


def index_files_in_blob_storage(blobs_metadata):
    """
    Based on the blob metadata for the existing blobs in Azure Blob Storage
    Index each file in the list of blobs

    :param {} blobs_metadata:
        Blob Metadata based on the existing file(s) in Azure Blob Storage
        See get_blobs_metadata()
    """
    for blob_metadata in blobs_metadata:
        response_json = create_index_azure(
            file_md5=blob_metadata["md5"],
            file_size=int(blob_metadata["size"]),
            file_name=blob_metadata["filename"],
            url=blob_metadata["urls"],
            authz=blob_metadata["authz"],
            acl=blob_metadata["acl"],
        )

        # capture the indexd did metadata
        object_id = response_json["did"]
        blob_metadata["guid"] = object_id

    return blobs_metadata


def submit_metadata_to_graph(blobs_metadata):
    """
    Based on the blob metadata for the existing blobs in Azure Blob Storage
    Submit metadata to Sheepdog to wrap the files in Azure Blob Storage

    Note that the `blob_metadata` should have a guid which
    captures the indexed file's documentid (`did`) or baseid

    :param {} blobs_metadata:
        Blob Metadata based on the existing file(s) in Azure Blob Storage
        See index_files_in_blob_storage() and get_blobs_metadata()
    """
    for blob_metadata in blobs_metadata:
        object_id = blob_metadata["guid"]
        md5sum = blob_metadata["md5"]
        file_name = os.path.basename(blob_metadata["filename"])
        file_size = int(blob_metadata["size"])

        submit_metadata_sheepdog(file_name, md5sum, file_size, object_id)


def get_presigned_urls(blobs_metadata):
    """
    For each of the indexed files from the existing blobs in Azure Blob Storage
    Get a presigned URL from fence using the `az` protocol

    This should occur after the indexed files have metadata in Sheepdog
    :param {} blobs_metadata:
        Blob Metadata based on the existing file(s) in Azure Blob Storage
        See submit_metadata_to_graph() and get_blobs_metadata()
    """
    # check for presigned urls
    presigned_urls = []
    for blob_metadata in blobs_metadata:
        # check for presigned url
        presigned_url = get_presigned_url(blob_metadata["guid"], "az")
        presigned_urls.append(presigned_url)
    return presigned_urls


def onboard_data_files_in_blob():
    """
    Onboard existing files in Azure Blob Storage
    Be sure to set the .env settings with the appropriate values

    This will attempt to read the existing files from Azure Blob Storage
    Then index the (filtered) existing files from Azure Blob storage
    After, you can submit the indexed files to Sheepdog wrapped in metadata
    Then you can get the presigned URLs for the indexed files

    """
    print("Get blob metadata")
    blobs_metadata = get_blobs_metadata()

    print("index blobs")
    index_files_in_blob_storage(blobs_metadata=blobs_metadata)

    print("submit metadata to graph")
    # submit metadata
    submit_metadata_to_graph(blobs_metadata=blobs_metadata)

    print("get presigned urls")
    # check for presigned urls
    presigned_urls = get_presigned_urls(blobs_metadata=blobs_metadata)
    print(f"got presigned_urls {presigned_urls}")


def main():
    print("Starting at " + time.strftime("%Y-%m-%d %H:%M") + "\n")

    onboard_data_files_in_blob()


if __name__ == "__main__":
    main()
