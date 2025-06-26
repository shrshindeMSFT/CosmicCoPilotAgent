def get_cluster_events_tool(cluster: str, from_date: str, to_date: str):
    """
    This tool retrieves os patching events in the cluster.
    It is used to track modifications and updates within the cluster environment.
    Args:
        cluster (str): The name of the cluster to query.
        from_date (str): The start date for the query in ISO format (YYYY-MM-DD HH:TT:SS).
        to_date (str): The end date for the query in ISO format (YYYY-MM-DD HH:TT:SS)
    Returns:
        KustoClient: An instance of KustoClient with the results of the query.
    """
    cmdw = "https://cosmic-test-dw.eastus.kusto.windows.net/"
    query =f"""
        let clusterID = cluster_cluster_global| where metadata.name == "{cluster}" | take 1|summarize  by id;
        cluster_upgrade_history_global
        | where spec.resourceType == "Cluster" and spec.resourceId == toscalar(clusterID) and tobool(spec.active) and isnull(metadata.deleteTimestamp)
        | where _kustoTime between (datetime({from_date}) .. datetime({to_date}))
        | order by id, _kustoTime asc
        | extend Time = _kustoTime, Train = tostring(spec.upgradeType), Build = tostring(spec.version), IsStart = row_number(1, prev(id) != id) == 1, IsEnd = next(id) != id
        | extend Type = case(
        IsStart, "Start",
        tobool(status.success) and IsEnd, "End",
        not(tobool(status.success)) and IsEnd, "Stopped",
        ""
        )| where isnotempty(Type)
        | summarize by Train, Build, Type,Time
        """

    query = query.replace(r"\|", r"|")
   
    # cosmic-msit-spo01-001-northcentralus-aks
    print(f"Querying cluster {cluster} for changes from {from_date} to {to_date}")
    kustoUtil = KustoUtils(cluster_url=cmdw, database_name="CosmicInventoryHistory")
    # kustoClient = KustoClient(cmdw)
    
    try:
        print(f"Executing query...")
        # result = kustoUtil.query(query=query).to_json(date_format="iso", indent=2)
        result = kustoUtil.query(query=query).to_string()
    except Exception as e:
        print(f"Error querying cluster {cluster}: {e}")
        return None
    
    print(f"Found2 {result} changes in cluster {cluster} from {from_date} to {to_date}")
    return result


"""
KustoUtils: A utility class for interacting with Azure Kusto databases.
"""
from enum import Enum
import subprocess
import sys
from typing import Any
try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

try:
    from azure.identity              import ChainedTokenCredential, DefaultAzureCredential
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "azure-identity"])
    from azure.identity              import ChainedTokenCredential, DefaultAzureCredential

try:
    from azure.kusto.data            import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoThrottlingError, KustoAuthenticationError
    from azure.kusto.data.response   import KustoResponseDataSet
except ImportError:
    # this also brings azure.kusto.data.exceptions and azure.kusto.data.response
    subprocess.check_call(["uv", "add", "azure-kusto-data"])
    from azure.kusto.data            import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoThrottlingError, KustoAuthenticationError
    from azure.kusto.data.response   import KustoResponseDataSet

try:
    from retry import retry
except ImportError:
    subprocess.check_call(["uv", "add", "retry"])
    from retry import retry

# ANSI color codes for terminal output
GREEN    = "\033[92m"
CYAN     = "\033[96m"
YELLOW   = "\033[0;33m"
RED      = "\033[0;31m"
NO_COLOR = "\033[0m"

class KustoUtilsError(Exception):
    """
    Custom exception class for Kusto utility errors.

    This exception is raised when an error specific to Kusto utilities occurs.
    It inherits from the built-in Exception class.
    """

class KustoDatabases(Enum):
    """
    Enum class representing different Kusto databases.

    Attributes:
        EV2 (int): Represents the EV2 database.
        ICM (int): Represents the ICM database.
        GITHUB (int): Represents the GITHUB database.
        AZUREDEVOPS (int): Represents the Azure DevOps database.
        COSMIC (int): Represents the COSMIC database.
        OCE (int): Represents the OCE database, used for On-Call information.
        COSMIC_INVENTORY (int): Represents the COSMIC Inventory database.
    """
    EV2 = 1,
    ICM = 2,
    GITHUB = 3,
    AZUREDEVOPS = 4,
    COSMIC = 5,
    OCE = 6, # For On-Call information
    COSMIC_INVENTORY = 7,
    EV2FAIRFAX = 8,
    EV2MOONCAKE = 9,

class KustoUtils:
    """
    A utility class for interacting with Azure Kusto databases.

    This class provides utility functions for executing Kusto queries and retrieving database
    schema.

    Most importantly, all results are returned as pandas DataFrames for easy data manipulation.

    Attributes:
        cluster_url (str): The URL of the Azure Kusto cluster.
        database_name (str): The name of the Azure Kusto database.
        client (KustoClient): An instance of KustoClient for executing queries.
        version (KustoResponseDataSet): The version information of the Azure Kusto database.
    """

    def __init__(self, cluster_url: str, database_name: str):
        """
        Initialize a KustoUtils instance.

        Parameters:
            cluster_url (str): The URL of the Azure Kusto cluster.
            database_name (str): The name of the Azure Kusto database.
        """
        self.cluster_url: str = cluster_url
        self.database_name: str = database_name
        self.credential: ChainedTokenCredential = KustoUtils.connect_to_azure(cluster_url)

    @staticmethod
    def qquery(db:KustoDatabases, query:str) -> pd.DataFrame:
        """
        Executes a Kusto query on the specified database and returns result as a pandas DataFrame.

        Parameters:
            db (KustoDatabases): The Kusto database to connect to.
            query (str): The Kusto query to be executed
        """
        return KustoUtils.get_client(db).query(query)

    @staticmethod
    def qquery_as_list_of_dict(db:KustoDatabases, query:str) -> list[dict[str, Any]]:
        """
        Executes a Kusto query on the specified database and returns the result as list of dict.

        Parameters:
            db (KustoDatabases): The Kusto database to connect to.
            query (str): The Kusto query to be executed
        """
        return KustoUtils.get_client(db).query(query=query).to_dict(orient='records')

    @staticmethod
    def get_ev2_client(rolloutinfra:str) -> 'KustoUtils':
        """
        Returns a KustoUtils instance for the EV2 database.
        parameters:
            rolloutinfra (str): The rollout infrastructure to connect to. 
            Can be "fairfax" or "mooncake" or whatever else.
        Returns:
            KustoUtils: A KustoUtils instance for the EV2 database.
        """
        rolloutinfra = rolloutinfra.lower()
        if rolloutinfra == "fairfax":
            return KustoUtils.get_client(KustoDatabases.EV2FAIRFAX)
        elif rolloutinfra == "mooncake":
            return KustoUtils.get_client(KustoDatabases.EV2MOONCAKE)
        else:
            return KustoUtils.get_client(KustoDatabases.EV2)

    @staticmethod
    def get_client(db:KustoDatabases) -> 'KustoUtils':
        """
        Returns a KustoUtils instance for the specified Kusto database.

        Parameters:
            db (KustoDatabases): The Kusto database to connect to.

        Returns:
            KustoUtils: A KustoUtils instance for the specified Kusto database.
        """
        ku = None
        if db == KustoDatabases.EV2:
            ku = KustoUtils(
                cluster_url="https://admcluster.kusto.windows.net",
                database_name="ADMDatabase"
            )
        elif db == KustoDatabases.EV2FAIRFAX:
            ku = KustoUtils(
                cluster_url="https://admcluster.kusto.windows.net",
                database_name="ADMFFDatabase"
            )
        elif db == KustoDatabases.EV2MOONCAKE:
            ku = KustoUtils(
                cluster_url="https://admcluster.kusto.windows.net",
                database_name="ADMMCDatabase"
            )
        elif db == KustoDatabases.ICM:
            ku = KustoUtils(
                cluster_url="https://icmcluster.kusto.windows.net",
                database_name="IcMDataWarehouse"
            )
        elif db == KustoDatabases.GITHUB:
            ku = KustoUtils(
                cluster_url="https://1es.kusto.windows.net",
                database_name="GitHub.EMU"
            )
        elif db == KustoDatabases.AZUREDEVOPS:
            ku = KustoUtils(
                cluster_url="https://1es.kusto.windows.net",
                database_name="AzureDevOps"
            )
        elif db == KustoDatabases.COSMIC:
            ku = KustoUtils(
                cluster_url="https://m365cosmicmonitoring.francecentral.kusto.windows.net",
                database_name="Cosmic"
            )
        elif db == KustoDatabases.COSMIC_INVENTORY:
            ku = KustoUtils(
                cluster_url="https://cosmic-prod-dw.westus2.kusto.windows.net",
                database_name="CosmicInventoryHistory"
            )
        elif db == KustoDatabases.OCE:
            ku = KustoUtils(
                cluster_url="https://icmcluster.kusto.windows.net",
                database_name="DirectoryServicePROD"
            )
        else:
            raise KustoUtilsError(f"Invalid Kusto database: {db}")
        return ku

    def get_kusto_database_schema(self) -> pd.DataFrame:
        """
        Retrieves the schema of the specified Kusto database.

        Returns:
            A pandas DataFrame containing the schema of the database.
        """
        query = f".show database schema | extend ClusterUrl='{self.cluster_url}'"
        with self._get_kusto_client() as client:
            return self._get_kusto_resultset(
                KustoUtils.execute_query(client=client, database=self.database_name, query=query)
            )

    def query(self, query: str) -> pd.DataFrame:
        """
        Executes a Kusto query on the specified database and returns the result as a pandas
        DataFrame.

        Parameters:
            query (str): The Kusto query to be executed.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the result of the Kusto query.
        """
        query = query.replace(r"\|", r"|")
        try:
            with self._get_kusto_client() as client:
                return self._get_kusto_resultset(
                    KustoUtils.execute_query(
                        client=client, database=self.database_name, query=query
                    )
                )
        except Exception as e:
            raise KustoUtilsError(
                f"Error executing query: {e}\n\n"
                f"Cluster URL: {self.cluster_url}\n"
                f"Database Name: {self.database_name}\n"
                f"Query: \n{query}"
            ) from e

    def _get_kusto_resultset(self, kusto_response: KustoResponseDataSet) -> pd.DataFrame:
        """
        Converts the Kusto response dataset to a pandas DataFrame.

        Parameters:
            kusto_response (KustoResponseDataSet): The Kusto response dataset.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the result of the Kusto query.
        """
        return pd.DataFrame.from_records(
            [row.to_dict() for row in kusto_response.primary_results[0]]
        )

    def _get_kusto_client(self) -> KustoClient:
        """
        Creates and returns a KustoClient instance using the provided Azure credentials.

        This method checks if the Azure credentials are available and raises
        an exception if they are not.
        It then creates a KustoClient instance using the KustoConnectionStringBuilder with
        the provided cluster URL and Azure credentials.

        Parameters:
        self (KustoUtils): The instance of KustoUtils.

        Returns:
        KustoClient: An instance of KustoClient for executing queries.

        Raises:
        KustoUtilsError: If the Azure credentials are not available.
        """
        if self.credential is None:
            raise KustoUtilsError(
                "Access token is empty, please create this object "
                "again with proper Azure connection by logging "
                "in to Azure with az login or Connect-AzAccount -UseDeviceAuthentication"
            )
        return KustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(
                connection_string=self.cluster_url, credential=self.credential
            )
        )

    @retry(KustoThrottlingError, tries=8, delay=20, backoff=10, logger=None)
    @staticmethod
    def execute_query(client: KustoClient, query: str, database) -> KustoResponseDataSet:
        """
        Executes the provided query on the specified database and returns the result as a
        KustoResponseDataSet.

        Parameters:
            query (str): The Kusto query to be executed.

        Returns:
            KustoResponseDataSet: The result of the Kusto query.
        """
        return client.execute(database=database, query=query)

    @staticmethod
    def connect_to_azure(token_request_context: str) -> ChainedTokenCredential:
        """
        Connects to Azure using the provided token_request_context and returns a
        ChainedTokenCredential instance.

        This function initializes a DefaultAzureCredential instance and attempts to create a
        KustoClient instance using the provided token_request_context and the
        DefaultAzureCredential.
        If successful, it returns the initialized credential.
        If an exception occurs, it returns None.

        Parameters:
        token_request_context (str): The connection string for the Azure Kusto cluster.

        Returns:
        ChainedTokenCredential: An instance of ChainedTokenCredential if the connection is
        successful. None if an exception occurs.
        """
        token_request_context = token_request_context.replace(r"https:\\\\", "https://")
        credential = DefaultAzureCredential()
        try:
            with KustoClient(
                KustoConnectionStringBuilder.with_azure_token_credential(
                    connection_string=token_request_context, credential=credential
                )
            ) as client:
                _ = KustoUtils.execute_query(
                    client=client, database="default", query=".show version"
                )
        except KustoAuthenticationError as ke:
            raise KustoUtilsError(
                f"Failed to connect to Azure cluster {token_request_context}."
                " Please make sure you are connected to Azure "
                "(either run az login or Connect-AzAccount -UseDeviceAuthentication) "
                f"with exception {ke}."
            ) from ke
        except Exception as e:
            raise KustoUtilsError(
                f"Could not perform query, got exception \n{e}"
            ) from e
        return credential

def kusto_query_to_json(cluster_url:str, database_name:str, query:str) -> None:
    """
    Executes a Kusto query on the specified database and returns the result as a pandas DataFrame.

    Parameters:
        cluster_url (str): The URL of the Azure Kusto cluster.
        database_name (str): The name of the Azure Kusto database.
        query (str): The Kusto query to be executed.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the result of the Kusto query.
    """
    print(KustoUtils(cluster_url, database_name).query(query).to_json(date_format="iso", indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="A tool for executing Kusto queries and retrieving database schema."
    )
    parser.add_argument(
        "--cluster-url",
        "-c",
        required=True,
        type=str,
        help="The URL of the Azure Kusto cluster."
    )
    parser.add_argument(
        "--database-name",
        "-d",
        required=True,
        type=str,
        help="The name of the Azure Kusto database.",
    )
    parser.add_argument(
        "--query",
        "-q",
        required=True,
        type=str,
        help="The Kusto query to be executed, can be a query string\n"
        "or the name of a file with extension .kql.",
    )
    parser.add_argument(
        "--output-format",
        "-o",
        required=False,
        type=str,
        help="Output format, must be JSON or CSV.",
        default="JSON",
        choices=["JSON", "CSV"],
    )
    parser.add_argument(
        "--to-file",
        "-t",
        required=False,
        type=str,
        help="File name to output, default is None.",
        default="",
    )
    args = parser.parse_args()
    kusto_query_text = args.query
    if kusto_query_text.endswith(".kql"):
        with open(kusto_query_text, encoding='utf-8') as file:
            kusto_query_text = file.read()
    result = KustoUtils(args.cluster_url, args.database_name).query(kusto_query_text)
    if args.to_file:
        if args.output_format == "JSON":
            result.to_json(args.to_file, orient="records", date_format="iso", indent=2)
        elif args.output_format == "CSV":
            result.to_csv(args.to_file, index=False)
    elif args.output_format == "JSON":
        print(result.to_json(date_format="iso", indent=2))
    elif args.output_format == "CSV":
        print(result.to_csv(index=False))

