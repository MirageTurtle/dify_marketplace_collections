import httpx
from typing import Optional
import json
from pathlib import Path
import sys
import logging
import os
import time
import random


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


CATEGORIES = [
    "model",
    "tool",
    "agent-strategy",
    "extension",
    "bundle",
]
DATA_DIR = Path("./data")
DIFFYPKG_DIR = Path("./difypkg")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DIFFYPKG_DIR, exist_ok=True)


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


def get_plugins_info_by_category(
    session: httpx.Client, category: str, timeout: int = 10
) -> tuple[bool, list[dict], Optional[str]]:
    """
    Get all plugin info by category.
    :param session: HTTP session
    :param category: Category of the plugin
    :param timeout: Timeout in seconds
    :return: Tuple of status, list of plugins, and error message
    """
    plugins = []
    url = "https://marketplace.dify.ai/api/v1/plugins/search/advanced"
    payload = {
        "page": 1,
        "page_size": 40,
        "query": "",
        "sort_by": "install_count",
        "sort_order": "DESC",
        "category": category,
        "tags": [],
        "type": "plugin",
    }
    response = session.post(url, timeout=timeout, json=payload)
    while response.status_code == 200:
        _total = response.json()["data"]["total"]
        if len(plugins) < _total:
            _plugins = response.json()["data"]["plugins"]
            plugins.extend(_plugins)
            logger.info(
                f"Retrieved {len(_plugins)} plugins at this iteration, total {len(plugins)}/{_total} plugins."
            )
            random_sleep()
        else:
            break
        payload["page"] += 1
        response = session.post(url, timeout=timeout, json=payload)
    if response.status_code == 200:
        return True, plugins, None
    else:
        return False, [], response.text


def save_plugin_info(
    plugins_info: list[dict], file_path: Path = Path("./plugins.json")
) -> None:
    """
    Save the plugin info to a JSON file.
    :param plugins_info: List of plugin info
    :param file_path: File path to save the collections
    :Note: It does not check the file path (validity, format, parent directory, etc.)
    """
    # If file_path is default, show a warning message
    _defualt_file_path = Path("./plugins.json")
    if file_path.resolve() == _defualt_file_path.resolve():
        logger.warning("You are saving to the default file '{_defualt_file_path}'.")
    with open(file_path, "w") as f:
        f.write(json.dumps(plugins_info, indent=4, ensure_ascii=False))
    logger.info(f"Saved plugin info to '{file_path}'.")


def get_single_collection(
    session: httpx.Client, collection_name: str, timeout: int = 10
) -> tuple[bool, list[dict], Optional[str]]:
    """
    DEPRECATED:
    Because I change the strategy to get all plugins by category.

    Get a collection by name.
    :param session: HTTP session
    :param collection_name: Name of the collection
    :param timeout: Timeout in seconds
    :return: Tuple of status, collection, and error message
    """
    # deprecated information
    logger.warning("You are using a deprecated function 'get_single_collection'.")
    logger.warning("This function is deprecated because the change in strategy.")
    logger.warning("You are advised to use 'get_plugins_info_by_category' instead.")
    url = f"https://marketplace.dify.ai/api/v1/collections/{collection_name}/plugins"
    payload = "{}"  # Empty payload (string)
    response = session.post(url, timeout=timeout, data=payload)
    if response.status_code == 200:
        collection = response.json()
        collection = collection["data"]["plugins"]
        return True, collection, None
    else:
        return False, [], response.text


def save_collection(
    collection: list[dict], file_path: Path = Path("./collection.json")
) -> None:
    """
    DEPRECATED:
    Because I change the strategy to get all plugins by category.

    Save a collection to a JSON file.
    :param collection: Collection
    :param file_path: File path to save the collection
    :Note: It does not check the file path (validity, format, parent directory, etc.)
    """
    # deprecated information
    logger.warning("You are using a deprecated function 'save_collection'.")
    logger.warning("This function is deprecated because the change in strategy.")
    logger.warning("You are advised to use 'save_plugins_info_by_category' instead.")
    # If file_path is default, show a warning message
    _defualt_file_path = Path("./collection.json")
    if file_path.resolve() == _defualt_file_path.resolve():
        logger.warning("You are saving to the default file '{_defualt_file_path}'.")
    with open(file_path, "w") as f:
        f.write(json.dumps(collection, indent=4, ensure_ascii=False))


def download_difypkg(
    plugin_id: str,
    plugin_version: str,
    hash: str,
    difypkg_dir: Path = Path("./difypkg"),
    timeout: int = 10,
) -> tuple[Optional[bool], Optional[str]]:
    """
    Download a Difypkg file.
    :param plugin_id: ID of the plugin
    :param plugin_version: Version of the plugin
    :param hash: Hash of the plugin
    :param difypkg_dir: Directory to save the Difypkg file
    :param timeout: Timeout in seconds
    :return: Tuple of status and error message
    """
    difypkg_file = (
        difypkg_dir / f"{plugin_id.replace("/", "_")}_{plugin_version}_{hash}.difypkg"
    )
    if difypkg_file.exists():
        logger.info(f"Difypkg file '{difypkg_file}' already exists.")
        return None, None
    url = f"https://marketplace.dify.ai/api/v1/plugins/{plugin_id}/{plugin_version}/download"
    try:
        response = httpx.get(url, timeout=timeout)
        if response.status_code == 200:
            with open(difypkg_file, "wb") as file:
                file.write(response.content)
            logger.info(f"Downloaded difypkg file '{difypkg_file}'.")
            return True, None
        else:
            logger.error(f"Error with url '{url}': {response.text}")
            return False, response.text
    except Exception as e:
        logger.error(f"Error with url '{url}': {e}")
        return False, str(e)


def get_plugin_info(plugin: dict) -> tuple[str, str, str]:
    """
    Get the name and description of a collection.
    :param plugin: Plugin
    :return: Tuple of plugin_id, version, and hash
    """
    latest_identifier = plugin["latest_package_identifier"]
    plugin_id = latest_identifier.split(":")[0]
    version = latest_identifier.split(":")[-1].split("@")[0]
    hash = latest_identifier.split("@")[-1]
    return plugin_id, version, hash


def random_sleep(min: int = 1, max: int = 3) -> None:
    time.sleep(random.randint(min, max))


if __name__ == "__main__":
    session = init_session()
    for category in CATEGORIES:
        # Get all plugins info by category
        status, plugins_info, error = get_plugins_info_by_category(session, category)
        random_sleep()
        if status:
            logger.info(
                f"Retrieved {len(plugins_info)} plugins for category '{category}'"
            )
            save_plugin_info(plugins_info, DATA_DIR / f"{category}.json")
        else:
            logger.error(f"Error: {error}")
            continue
        for plugin in plugins_info:
            # Get plugin info and download difypkg
            plugin_id, version, hash = get_plugin_info(plugin)
            os.makedirs(DIFFYPKG_DIR / category, exist_ok=True)
            status, error = download_difypkg(
                plugin_id, version, hash, DIFFYPKG_DIR / category
            )
            if status is None:
                continue
            else:
                random_sleep()
            
    session.close()
