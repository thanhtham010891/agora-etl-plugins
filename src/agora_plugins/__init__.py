"""agora-etl-plugins — official plugins for agora-etl."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("agora-etl-plugins")
except PackageNotFoundError:
    __version__ = "0+unknown"
