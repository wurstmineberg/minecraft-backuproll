
__version__ = None
try:
    from backuproll._version import version as __version__
except ImportError:
    from setuptools_scm import get_version
    __version__ = get_version(root='..', relative_to=__file__)

