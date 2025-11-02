import aiohttp
import asyncio
from typing import Optional
import json
from pathlib import Path
import sys
import logging
import os
import time
import random
from concurrent.futures import ProcessPoolExecutor
import multiprocessing


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


def get_session_headers():
    """
    Get HTTP session headers.
    """
    return {
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


async def get_plugins_info_by_category(
    session: aiohttp.ClientSession, category: str, timeout: int = 10
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
    logger.info(f"Retrieving plugins for category '{category}'...")
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with session.post(url, timeout=timeout_obj, json=payload) as response:
        response_json = await response.json()
        _total = response_json["data"]["total"]
        status_code = response.status
        response_text = await response.text()

    logger.info(f"Total {_total} plugins found for category '{category}'.")
    while status_code == 200:
        _plugins = response_json["data"]["plugins"]

        if _plugins is None or len(_plugins) == 0:
            logger.info("No more plugins found, ending retrieval.")
            if len(plugins) < _total:
                logger.warning(
                    f"Expected {_total} plugins, but only retrieved {len(plugins)} plugins."
                )
            break

        if len(plugins) < _total:
            plugins.extend(_plugins)
            logger.info(
                f"Retrieved {len(_plugins)} plugins at this iteration, total {len(plugins)}/{_total} plugins."
            )
            await asyncio.sleep(random.randint(1, 3))
        else:
            break
        payload["page"] += 1
        async with session.post(url, timeout=timeout_obj, json=payload) as response:
            response_json = await response.json()
            status_code = response.status
            response_text = await response.text()

    if status_code == 200:
        return True, plugins, None
    else:
        return False, [], response_text


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


async def get_single_collection(
    session: aiohttp.ClientSession, collection_name: str, timeout: int = 10
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
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with session.post(url, timeout=timeout_obj, data=payload) as response:
        if response.status == 200:
            response_json = await response.json()
            collection = response_json["data"]["plugins"]
            return True, collection, None
        else:
            response_text = await response.text()
            return False, [], response_text


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


async def download_difypkg(
    session: aiohttp.ClientSession,
    plugin_id: str,
    plugin_version: str,
    hash: str,
    difypkg_dir: Path = Path("./difypkg"),
    timeout: int = 10,
) -> tuple[Optional[bool], Optional[str]]:
    """
    Download a Difypkg file.
    :param session: HTTP session
    :param plugin_id: ID of the plugin
    :param plugin_version: Version of the plugin
    :param hash: Hash of the plugin
    :param difypkg_dir: Directory to save the Difypkg file
    :param timeout: Timeout in seconds
    :return: Tuple of status and error message
    """
    difypkg_file = (
        difypkg_dir / f"{plugin_id.replace('/', '_')}_{plugin_version}_{hash}.difypkg"
    )
    if difypkg_file.exists():
        logger.info(f"Difypkg file '{difypkg_file}' already exists.")
        return None, None
    url = f"https://marketplace.dify.ai/api/v1/plugins/{plugin_id}/{plugin_version}/download"
    try:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with session.get(url, timeout=timeout_obj) as response:
            if response.status == 200:
                content = await response.read()
                with open(difypkg_file, "wb") as file:
                    file.write(content)
                logger.info(f"Downloaded difypkg file '{difypkg_file}'.")
                return True, None
            else:
                response_text = await response.text()
                logger.error(f"Error with url '{url}': {response_text}")
                return False, response_text
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


async def process_category(category: str) -> None:
    """
    Process a single category: retrieve plugin info and download packages.
    This function is designed to run in a separate process.

    :param category: Category to process
    """
    # Configure logging for this process
    process_logger = logging.getLogger(f"{__name__}.{category}")
    process_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    process_logger.addHandler(stream_handler)

    process_logger.info(f"Starting processing for category '{category}'")

    headers = get_session_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        # Get all plugins info by category
        status, plugins_info, error = await get_plugins_info_by_category(
            session, category
        )
        await asyncio.sleep(random.randint(1, 3))
        if status:
            process_logger.info(
                f"Retrieved {len(plugins_info)} plugins for category '{category}'"
            )
            save_plugin_info(plugins_info, DATA_DIR / f"{category}.json")
        else:
            process_logger.error(f"Error: {error}")
            return

        # Download all plugins for this category concurrently
        os.makedirs(DIFFYPKG_DIR / category, exist_ok=True)
        download_tasks = []
        for plugin in plugins_info:
            # Get plugin info and download difypkg
            plugin_id, version, hash = get_plugin_info(plugin)
            download_tasks.append(
                download_difypkg(
                    session, plugin_id, version, hash, DIFFYPKG_DIR / category
                )
            )

        # Execute all downloads concurrently with a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent downloads

        async def download_with_semaphore(task):
            async with semaphore:
                result = await task
                if result[0] is not None:  # Only sleep if download was attempted
                    await asyncio.sleep(random.randint(1, 3))
                return result

        results = await asyncio.gather(
            *[download_with_semaphore(task) for task in download_tasks],
            return_exceptions=True,
        )

        # Log any errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                process_logger.error(f"Download task {i} failed with exception: {result}")

        process_logger.info(f"Completed processing for category '{category}'")


def process_category_wrapper(category: str) -> str:
    """
    Wrapper function to run async process_category in a separate process.

    :param category: Category to process
    :return: Category name (for tracking completion)
    """
    asyncio.run(process_category(category))
    return category


async def main():
    """
    Main async function to retrieve and download plugins using multiprocessing.
    """
    logger.info(f"Starting multiprocessing with {len(CATEGORIES)} categories")

    # Use ProcessPoolExecutor to process categories in parallel
    with ProcessPoolExecutor(max_workers=len(CATEGORIES)) as executor:
        # Submit all category processing tasks
        futures = [executor.submit(process_category_wrapper, category) for category in CATEGORIES]

        # Wait for all processes to complete
        for future in futures:
            try:
                completed_category = future.result()
                logger.info(f"Category '{completed_category}' processing completed")
            except Exception as e:
                logger.error(f"Category processing failed with exception: {e}")

    logger.info("All categories processed successfully")


if __name__ == "__main__":
    # Required for multiprocessing on Windows and macOS
    multiprocessing.set_start_method('spawn', force=True)
    asyncio.run(main())
