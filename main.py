import httpx
from typing import Optional
import json
from pathlib import Path
import sys
# import concurrent.futures
# from functools import partial
import logging
import os
import time


logger = logging.getLogger(__name__)


def init_session():
    """
    Initialize a HTTP session with headers.
    """
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://marketplace.dify.ai/",
        "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Cookie": "locale=en-US",
    }
    session = httpx.Client()
    session.headers.update(headers)
    return session

def get_collections(session: httpx.Client, timeout: int = 10) -> tuple[bool, list[dict], Optional[str]]:
    """
    Get all collections from the marketplace.
    :param session: HTTP session
    :param timeout: Timeout in seconds
    :return: Tuple of status, collections, and error message
    """
    url = "https://marketplace.dify.ai/api/v1/collections?page=1&page_size=100"
    response = session.get(url, timeout=timeout)
    if response.status_code == 200:
        collections = response.json()
        collections = collections["data"]["collections"]
        return True, collections, None
    else:
        return False, [], response.text

def save_collections(collections: list[dict], file_path: Path =Path("./collections.json")) -> None:
    """
    Save collections to a JSON file.
    :param collections: List of collections
    :param file_path: File path to save the collections
    :Note: It does not check the file path (validity, format, parent directory, etc.)
    """
    # If file_path is "./collections.json", show a warning message
    if file_path.resolve() == Path("./collections.json").resolve():
        logger.warning("You are saving the default collections file 'collections.json'.")
    with open(file_path, "w") as f:
        f.write(json.dumps(collections, indent=4))

def get_single_collection(session: httpx.Client, collection_name: str, timeout: int = 10) -> tuple[bool, list[dict], Optional[str]]:
    """
    Get a collection by name.
    :param session: HTTP session
    :param collection_name: Name of the collection
    :param timeout: Timeout in seconds
    :return: Tuple of status, collection, and error message
    """
    url = f"https://marketplace.dify.ai/api/v1/collections/{collection_name}/plugins"
    payload = "{}" # Empty payload (string)
    response = session.post(url, timeout=timeout, data=payload)
    if response.status_code == 200:
        collection = response.json()
        collection = collection["data"]["plugins"]
        return True, collection, None
    else:
        return False, [], response.text

def save_collection(collection: list[dict], file_path: Path =Path("./collection.json")) -> None:
    """
    Save a collection to a JSON file.
    :param collection: Collection
    :param file_path: File path to save the collection
    :Note: It does not check the file path (validity, format, parent directory, etc.)
    """
    # If file_path is "./collection.json", show a warning message
    if file_path.resolve() == Path("./collection.json").resolve():
        logger.warning("You are saving the default collection file 'collection.json'.")
    with open(file_path, "w") as f:
        f.write(json.dumps(collection, indent=4))

def download_difypkg(plugin_name: str, plugin_version: str, difypkg_dir: Path =Path("./difypkg"), timeout: int = 10) -> tuple[bool, Optional[str]]:
    """
    Download a Difypkg file.
    :param plugin_name: Name of the plugin
    :param plugin_version: Version of the plugin
    :param difypkg_dir: Directory to save the Difypkg file
    :param timeout: Timeout in seconds
    :return: Tuple of status and error message
    """
    difypkg_file = difypkg_dir / f"{plugin_name.replace("/", "_")}_{plugin_version}.difypkg"
    if difypkg_file.exists():
        logger.info(f"Difypkg file '{difypkg_file}' already exists.")
        return True, None
    url = f"https://marketplace.dify.ai/api/v1/plugins/{plugin_name}/{plugin_version}/download"
    response = httpx.get(url, timeout=timeout)
    if response.status_code == 200:
        with open(difypkg_file, "wb") as file:
            file.write(response.content)
        return True, None
    else:
        return False, response.text

def get_plugin_info(plugin: dict) -> tuple[str, str]:
    """
    Get the name and description of a collection.
    :param plugin: Plugin
    :return: Tuple of plugin_id(name), version
    """
    return plugin["plugin_id"], plugin["latest_version"]

if __name__ == "__main__":
    session = init_session()
    # Get collections
    status, collections, error = get_collections(session)
    if status:
        save_collections(collections)
        logger.info("Collections saved successfully.")
    else:
        logger.error(f"Failed to get collections: {error}")
        sys.exit(1)
    time.sleep(1)
    # Get each collection and save it
    for collection in collections:
        status, collection_data, error = get_single_collection(session, collection["name"])
        if status:
            save_collection(collection_data, Path(f"./collections/{collection['name']}.json"))
            logger.info(f"Collection '{collection['name']}' saved successfully.")
            # Download difypkg files of each plugin in the collection
            difypkg_dir = Path(f"./difypkg/{collection['name']}")
            os.makedirs(difypkg_dir, exist_ok=True)
            for plugin in collection_data:
                plugin_id, version = get_plugin_info(plugin)
                status, error = download_difypkg(plugin_id, version, difypkg_dir)
                if status:
                    logger.info(f"Plugin '{plugin_id}' downloaded successfully.")
                else:
                    logger.error(f"Failed to download plugin '{plugin_id}': {error}")
                time.sleep(1)
        else:
            logger.error(f"Failed to get collection '{collection['name']}': {error}")
        time.sleep(1)
    session.close()
