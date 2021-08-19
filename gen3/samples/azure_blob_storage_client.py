"""
Set up the wrapper AzureBlobStorageClient
"""
import os
from urllib.parse import urlparse, ParseResult, urlunparse
from azure.storage.blob import BlobServiceClient


class AzureBlobStorageClient:
    """
    Wrapper Client for Azure Blob Storage
    """

    def __init__(
        self,
        connection_string=None,
        container_name=None,
        account_url=None,
        credential=None,
        starts_with_filter=None,
        commons_authz=None,
        commons_acl=None,
    ):
        """
        Initialize AzureBlobStorageClient

        :param str connection_string:
            Set the Azure Blob Storage connection string
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        :param str container_name:
            Set the Azure Blob Storage container name
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        :param str account_url:
            Set the Azure Blob Storage account_url
            This can be set to https://storageaccount.windows.blobs.net/
            The default is None
        :param str credential:
            This is the SAS token, can be used in conjunction with the account_url
            The default is None
        :param str starts_with_filter:
            This is used to get the list of blobs given a filter.
            For example, this can be set to `folder_1/` which will
            get the the list of blobs starting with the path of
            `folder_1`.
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        :param str commons_authz:
            This is for indexing the files with a given authz
            For example, this could be set to `/programs`
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        :param str commons_acl:
            This is for indexing the files with a given ACL
            For example, this could be set to `'*'`
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        """
        self._connection_string = (
            connection_string
            if connection_string
            else os.environ.get("AZURE_BLOB_STORAGE_CONNECTION_STRING")
        )
        self._container_name = (
            container_name
            if container_name
            else os.environ.get("AZURE_BLOB_STORAGE_CONTAINER_NAME")
        )
        self._starts_with_filter = (
            starts_with_filter
            if starts_with_filter
            else os.environ.get("STARTS_WITH_FILTER")
        )
        self._commons_authz = (
            commons_authz
            if commons_authz
            else os.environ.get("COMMONS_AUTHZ", "/programs")
        )
        self._commons_acl = (
            commons_acl if commons_acl else os.environ.get("COMMONS_ACL", "*")
        )
        self._account_url = account_url
        self._credential = credential

    def get_container_url(self):
        """
        Get container URL based on the environment settings for the
        Azure Blob Storage Container (AZURE_BLOB_STORAGE_CONTAINER_NAME)
        """
        blob_service_client = self.get_blob_service_client()

        container_client = blob_service_client.get_container_client(
            container=self._container_name
        )

        return container_client.url

    def get_blob_service_client(self, container_name=None, blob_name=None):
        """
        Get a blob service client to interact with Azure Blob Storage

        :param str container_name:
            Get an Azure Blob Service client
            for a given container name
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        :param str blob_name:
            Get an Azure Blob Service client
            for a given blob name
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        """
        blob_service_client = None

        if self._account_url and self._credential:
            # Attempt to use the credential and storage account url if available
            print(
                f"get_blob_service_client with account_url {self._account_url} and credential {self._credential}"
            )
            blob_service_client = BlobServiceClient(
                account_url=self._account_url, credential=self._credential
            )
        else:
            # Fall back to connection string
            print(
                f"get_blob_service_client with _connection_string {self._connection_string}"
            )
            blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=self._connection_string,
                container_name=container_name,
                blob_name=blob_name,
            )

        return blob_service_client

    def get_container_client(self, container_name=None):
        """
        Get a container client for Azure Blob Storage for a given container name
        The container doesn't need to exist yet

        :param str container_name:
            Get an Azure Blob Storage Container client
            for a given container name
            The default is None, which lets the
            AzureBlobStorageClient use the defaults set through the environment
        """
        container_name = container_name if container_name else self._container_name

        blob_service_client = self.get_blob_service_client()

        container_client = blob_service_client.get_container_client(
            container=container_name
        )

        return container_client

    def get_blobs(self, starts_with_filter=None):
        """
        Get a list of blobs from a designated container in
        Azure Blob Storage based on environment settings

        :param str starts_with_filter:
            Get blobs in the container that start with a given path
            e.g. this can be set to `folder_1/` to get blobs listed
            under `folder_1/`.
            Default value is None which will get a list of all blobs
        """
        starts_with_filter = (
            starts_with_filter if starts_with_filter else self._starts_with_filter
        )
        container_client = self.get_container_client()

        blobs = container_client.list_blobs(name_starts_with=starts_with_filter)

        return blobs

    def upload_file_content_to_blob(self, blob_name, container_name, source_file_path):
        """
        Upload a given source file to a container/blob

        :param str blob_name:
            Name of the blob to push blob content into
        :param str container_name:
            Name of the container to push the blob into
        :param str source_file_path:
            Local file path to use to read into the blob
        """
        blob_service_client = self.get_blob_service_client(
            container_name=container_name, blob_name=blob_name
        )
        blob = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        with open(source_file_path) as f:
            data = f.read()
            blob.upload_blob(data=data)

    def get_blob_content_as_file(self, blob_name, destination_file_path=None):
        """
        Get blob contents as a file for a given blob name and local destination file path

        :param str blob_name:
            Name of the existing blob to get blob content
        :param str scheme:
            This is the desired scheme to use with the URL
            E.g. change https://storageaccount/container/blob/file.txt to az://storageaccount/container/blob/file.txt
            If the scheme is set as `az`
        """

        blob_service_client = self.get_blob_service_client(
            container_name=self._container_name, blob_name=blob_name
        )
        blob = blob_service_client.get_blob_service_client(
            container=self._container_name, blob=blob_name
        )

        output_file_path = destination_file_path if destination_file_path else blob_name
        with open(output_file_path, "wb") as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)

        return output_file_path

    def _convert_url_to_scheme(self, url, scheme):
        """
        Convert a given URL to use a new scheme
        e.g. https://somecontainer/blob/file.txt will become az://somecontainer/blob/file.txt

        :param str url:
            The URL to convert, for example https://storageaccount/container/blob/file.txt
        :param str scheme:
            This is the desired scheme to use with the URL
            E.g. change https://storageaccount/container/blob/file.txt to az://storageaccount/container/blob/file.txt
            If the scheme is set as `az`
        """
        parsed_url = urlparse(url)
        new_parsed_url = ParseResult(
            scheme=scheme,
            netloc=parsed_url.netloc,
            path=parsed_url.path,
            params=parsed_url.params,
            query=parsed_url.query,
            fragment=parsed_url.fragment,
        )
        new_url = urlunparse(new_parsed_url)
        return new_url

    def prepare_blob_index_metadata(self, blobs, desired_scheme):
        """
        Get blob metadata from Azure Blob Storage given a list of blobs

        A row might look like the following:
        {
            "guid": None,  # Indexd needs to be able to create a document id
            "md5": str # this is the blob md5 from Azure Blob Storage
            "size": str # this is the blob size from Azure Blob Storage
            "authz": str # this is the authz set through the .env settings
            "acl": str # this is the acl set through the .env settings
            "urls": str # this is the converted url
                (e.g. https://container/blob/file.txt becomes az://container/blob/file.txt)
            "filename": str # this is the blob name,
        }

        :param generator blobs:
            This is a generator with a list of blob information
        :param str desired_scheme:
            This is the desired scheme to use with the existing URL, for use with `fence`
            E.g. change https://storageaccount/container/blob/file.txt to az://storageaccount/container/blob/file.txt
        """
        rows = []

        # Fetch blob metadata
        # guid	md5	size	authz	acl	urls	filename
        container_url = self.get_container_url()
        for blob in blobs:
            url = container_url + "/" + blob.name
            converted_url = self._convert_url_to_scheme(url, desired_scheme)

            row = {
                "guid": None,  # Indexd needs to be able to create a document id.  Currently getting 404's when submitting this through index_object_manifest on the gen3 index client.
                "md5": blob.content_settings.content_md5.hex(),
                "size": str(blob.size),
                "authz": self._commons_authz,
                "acl": self._commons_acl,
                "urls": converted_url,
                "filename": blob.name,
            }
            rows.append(row)

        # JSON payload that can be used
        return rows
