import logging
import time
from datetime import datetime
from typing import Any

import boto3
from botocore.client import Config
from sentinelhub import (
    ByocCollection,
    ByocCollectionAdditionalData,
    ByocCollectionBand,
    ByocTile,
    SentinelHubBYOC,
    SHConfig,
)


class TileListParameters:
    """
    Parameters for listing tiles from an S3 bucket.

    This class holds the parameters needed to connect to and list objects from
    an S3 bucket.

    bucket_url has no default: "eodata.dataspace.copernicus.eu" (CDSE's
    read-only archive mirror) and "s3.waw3-1.cloudferro.com" (CreoDIAS's
    writable object storage) are different backends with different S3
    semantics, and defaulting to one silently misdirects anyone using the
    other.
    """

    def __init__(
        self,
        base_path: str,
        bucket_name: str,
        creodias_username: str,
        creodias_password: str,
        bucket_url: str,
    ):
        self.bucket_url = bucket_url
        self.base_path = base_path
        self.bucket_name = bucket_name
        self.creodias_username = creodias_username
        self.creodias_password = creodias_password


class Ingestor:
    """
    Handles the ingestion of data into a BYOC (Bring Your Own COG) collection.

    This class manages the entire workflow of creating a collection, listing files,
    building tile information, and ingesting the data into Sentinel Hub.
    """

    def __init__(self, config: SHConfig):
        """
        Initialize the Ingestor with a Sentinel Hub configuration.

        Args:
            sentinelhub.config.SHConfig: A SentinelHub configuration object with authentication details
        """
        self.byoc_client = self.initialise_byoc_client(config)
        self.byoc_collection = None
        self.file_list = []

    def initialise_byoc_client(self, config: SHConfig) -> SentinelHubBYOC:
        """
        Create and return a SentinelHubBYOC client.

        Args:
            sentinelhub.config.SHConfig: A SentinelHub configuration object with authentication details

        Returns:
            sentinelhub.api.byoc.SentinelHubBYOC: A client for interacting with the BYOC API
        """
        return SentinelHubBYOC(config=config)

    def create_byoc_collection(
        self,
        collection_name: str,
        bucket_name: str,
        storage_id: str | None = None,
        band_information: list[dict[str, Any]] | None = None,
    ) -> None:
        """
        Create a new BYOC collection in Sentinel Hub.

        This function creates a new collection with the specified name and associates
        it with the given S3 bucket. Optionally, a storage identifier can be provided.

        Args:
            collection_name: The name for the new collection
            bucket_name: The S3 bucket name where data is stored
            storage_id: Optional storage identifier (e.g., "eodata" for CDSE)

        Returns:
            None: The created collection is stored in self.byoc_collection
        """
        if band_information is not None:
            # Define the bands
            band_parameters = {}
            # Define the required fields
            required_fields = ["name", "source", "bit_depth", "sample_format"]

            # Check for required fields in each band
            for band in band_information:
                missing_fields = [
                    field for field in required_fields if field not in band
                ]
                if missing_fields:
                    raise ValueError(
                        f"Each band must contain {', '.join(missing_fields)} fields"
                    )
                # Build the band parameters
                band_parameters[band["name"]] = ByocCollectionBand(
                    source=band["source"],
                    band_index=1,
                    bit_depth=band["bit_depth"],
                    sample_format=band["sample_format"],
                    no_data=band["no_data"] if "no_data" in band else None,
                )
            if storage_id:
                band_config = ByocCollectionAdditionalData(
                    bands=band_parameters, other_data={"storageIdentifier": storage_id}
                )
            else:
                band_config = ByocCollectionAdditionalData(
                    bands=band_parameters,
                )
        else:
            if storage_id:
                band_config = ByocCollectionAdditionalData(
                    other_data={"storageIdentifier": storage_id}
                )
            else:
                band_config = None

        new_collection = ByocCollection(
            name=collection_name, s3_bucket=bucket_name, additional_data=band_config
        )

        self.byoc_collection = self.byoc_client.create_collection(new_collection)
        time.sleep(5)

    def connect_to_existing_collection(self, collection_id: str) -> None:
        """
        Connect to an existing BYOC collection using its ID.

        This function retrieves an existing collection from Sentinel Hub using the
        provided collection ID and sets it as the current collection for this Ingestor.

        Args:
            collection_id: The UUID of the existing BYOC collection

        Returns:
            None: The existing collection is stored in self.byoc_collection

        Raises:
            The underlying exception from the BYOC client, unchanged
        """
        try:
            # Get the existing collection using the BYOC client
            self.byoc_collection = self.byoc_client.get_collection(collection_id)
        except Exception:
            logging.exception(f"Failed to connect to collection {collection_id}")
            raise

    def list_tiles(self, params: TileListParameters) -> list[str]:
        """
        List all TIFF files in the S3 bucket that match the given parameters.

        This function connects to an S3 bucket and walks the object tree below the
        specified base path, collecting all TIFF files (.tif or .tiff, any case)
        into self.file_list.

        The walk descends one level at a time rather than requesting a flat
        recursive listing. The eodata endpoint only returns entries directly below
        the queried prefix, so a flat listing returns nothing useful there.
        Delimiter="/" makes standard S3 endpoints (e.g. WAW3-1) behave the same
        way, so the same walk is correct on both and has no depth limit.

        Args:
            params: TileListParameters object containing S3 access information

        Returns:
            List[str]: The list of file paths found (also stored in self.file_list)

        Raises:
            ValueError: If no .tif/.tiff files are found under base_path
        """
        # Create a session and client
        session = boto3.session.Session()
        s3_client = session.client(
            "s3",
            endpoint_url=f"https://{params.bucket_url}",
            aws_access_key_id=params.creodias_username,
            aws_secret_access_key=params.creodias_password,
            config=Config(signature_version="s3v4"),
            verify=True,  # Check SSL certificate
        )

        # Use a paginator to handle large result sets
        paginator = s3_client.get_paginator("list_objects_v2")
        tiff_files = []
        pending = [params.base_path.rstrip("/") + "/"]
        visited = set()
        while pending:
            prefix = pending.pop()
            if prefix in visited:
                continue
            visited.add(prefix)
            for page in paginator.paginate(
                Bucket=params.bucket_name, Prefix=prefix, Delimiter="/"
            ):
                # Standard S3 rolls sub-folders into CommonPrefixes; eodata
                # returns them in Contents as keys ending in "/". Handle both.
                for common in page.get("CommonPrefixes", []):
                    pending.append(common["Prefix"])
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/"):
                        pending.append(key)
                    elif key.lower().endswith((".tif", ".tiff")):
                        tiff_files.append(key)

        if not tiff_files:
            raise ValueError(
                f"No .tif/.tiff files found under "
                f"s3://{params.bucket_name}/{params.base_path} - "
                "check bucket_name and base_path."
            )

        self.file_list = tiff_files
        return tiff_files

    def build_byoc_tiles(
        self, sensing_time_position: dict[str, Any], band_position: dict[str, Any]
    ) -> list[tuple[str, datetime]]:
        """
        Build BYOC tile from file paths.

        This function processes the file paths in self.file_list to extract datetime
        information and create tile paths with (BAND) placeholders that Sentinel Hub uses
        to identify the different bands of each tile.

        Args:
            sensing_time_position: Dictionary with parameters for extracting datetime:
                - path: Position of folder containing datetime in path
                - delimiter: Character separating parts of the folder name
                - position: Position of datetime part after splitting
                - format: Format string for datetime parsing (e.g., "%Y%m%d")
            band_position: Dictionary with parameters for identifying bands:
                - path: Which path element holds the band identifier (-1 for the
                  filename, -2 for its parent folder, and so on). Only the final
                  element is treated as having a file extension; every other
                  element of the path is left untouched.
                - delimiter: Character separating parts of that element
                - position: Position of band identifier after splitting

        Returns:
            List[Tuple[str, datetime]]: List of tuples containing:
                - Tile path with (BAND) placeholder
                - Datetime object representing sensing time
        """
        byoc_tiles = []
        for tile_path in self.file_list:
            # Get the sensing time from the tile path
            folder_name = tile_path.split("/")[sensing_time_position["path"]]

            # Handle empty or None delimiter for datetime extraction
            if sensing_time_position["delimiter"] in ("", None):
                # No delimiter, use the whole folder name
                parts = [folder_name]
            else:
                parts = folder_name.split(sensing_time_position["delimiter"])

            if isinstance(sensing_time_position["position"], int):
                datetime_str = parts[sensing_time_position["position"]]
            else:
                datetime_str = "".join(
                    [parts[i] for i in sensing_time_position["position"]]
                )

            datetime_obj = datetime.strptime(
                datetime_str, sensing_time_position["format"]
            )
            parts = tile_path.split("/")
            idx = band_position["path"]
            if not -len(parts) <= idx < len(parts):
                raise IndexError(
                    f"band_position['path']={idx} is outside the "
                    f"{len(parts)}-element path {tile_path!r}"
                )

            # Only the final element carries a file extension worth preserving.
            # A folder such as "..._V2.2.1_cog" must not be split on its dots.
            target = parts[idx]
            if idx in (-1, len(parts) - 1):
                name_parts = target.rsplit(".", 1)
                base_name = name_parts[0]
                extension = f".{name_parts[1]}" if len(name_parts) > 1 else ""
            else:
                base_name, extension = target, ""

            # Handle empty or None delimiter for band extraction
            if band_position["delimiter"] in ("", None):
                # No delimiter, cannot extract band - use entire base name as placeholder
                new_base_name = "(BAND)"
            else:
                # Replace band identifier in the base name
                split_file_name = base_name.split(band_position["delimiter"])
                try:
                    split_file_name[band_position["position"]] = "(BAND)"
                except IndexError:
                    raise IndexError(
                        f"band_position['position']={band_position['position']} is "
                        f"outside {target!r} split on {band_position['delimiter']!r}"
                    ) from None
                new_base_name = band_position["delimiter"].join(split_file_name)

            # Substitute in place, leaving every other path element untouched
            parts[idx] = new_base_name + extension
            byoc_path = "/".join(parts)

            if byoc_path not in [x[0] for x in byoc_tiles]:
                byoc_tiles.append([byoc_path, datetime_obj])

        return byoc_tiles

    def ingest_tiles_to_collection(
        self, sensing_time_position: dict[str, Any], band_position: dict[str, Any]
    ) -> None:
        """
        Ingest tiles into the BYOC collection.

        This function creates tiles in the Sentinel Hub BYOC collection based on the
        file paths discovered in the S3 bucket. It checks for existing tiles to avoid
        duplicates and only ingests new tiles.

        Args:
            sensing_time_position: Dictionary with parameters for extracting datetime
            band_position: Dictionary with parameters for identifying bands

        Returns:
            None

        Note:
            The collection must be created first (using create_byoc_collection)
            Tiles are created in Sentinel Hub but actual data ingestion happens asynchronously
        """
        byoc_tiles = self.build_byoc_tiles(sensing_time_position, band_position)
        # iter_tiles yields raw API payloads (JsonDict), not ByocTile objects
        existing_paths = {
            tile["path"] for tile in self.byoc_client.iter_tiles(self.byoc_collection)
        }
        for path, sensing_time in byoc_tiles:
            if path not in existing_paths:
                self.byoc_client.create_tile(
                    self.byoc_collection, ByocTile(path=path, sensing_time=sensing_time)
                )

    def collection_tile_report(self) -> tuple[dict[str, int], list[str]]:
        """
        Generate a report of the tile ingestion status in the collection.

        Returns:
            Tuple[Dict[str, int], List[str]]: A tuple containing:
                - A dictionary with counts of tiles by status (Ingested, Failed, Pending, Total)
                - A list of failure reasons for failed tiles
        """
        tiles = list(self.byoc_client.iter_tiles(self.byoc_collection))

        report = {"Ingested": 0, "Failed": 0, "Pending": 0, "Total": len(tiles)}
        failed = []

        for tile in tiles:
            if tile["status"] == "INGESTED":
                report["Ingested"] += 1
            elif tile["status"] == "FAILED":
                report["Failed"] += 1
                # additionalData may be absent or present-but-null; either
                # way there's no failure cause to report.
                additional_data = tile.get("additionalData") or {}
                if "failedIngestionCause" in additional_data:
                    failed.append(additional_data["failedIngestionCause"])
                else:
                    failed.append(f"Unknown failure for tile {tile['path']}")
            else:
                report["Pending"] += 1

        return report, failed
