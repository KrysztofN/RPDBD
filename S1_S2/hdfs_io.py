import tempfile
from pathlib import Path
from urllib.parse import quote
import requests
import xarray as xr
from config import WEBHDFS


def path_url(path):
    return WEBHDFS.rstrip('/') + '/webhdfs/v1' + quote(path, safe='/')


def read_bytes(path):
    response = requests.get(path_url(path), params={'op': 'OPEN', "user.name": "hadoop"}, timeout=240)
    response.raise_for_status()
    return response.content


def read_nc(path):
    raw = read_bytes(path)
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / 'source.nc'
        f.write_bytes(raw)
        with xr.open_dataset(f) as ds:
            return ds.load()


def write_bytes(path, data):
    # Create parent directory using the WebHDFS API.
    parent = str(Path(path).parent)
    result = requests.put(path_url(parent), params={'op': 'MKDIRS', "user.name": "hadoop"}, timeout=30)
    result.raise_for_status()
    # WebHDFS CREATE: requests follows the NameNode's 307 redirect to a DataNode.
    response = requests.put(path_url(path), params={'op': 'CREATE', 'overwrite': 'true', "user.name": "hadoop"},
                            data=data, timeout=240)
    response.raise_for_status()
