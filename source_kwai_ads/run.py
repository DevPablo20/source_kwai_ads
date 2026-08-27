import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceKwaiAds


def run():
    launch(SourceKwaiAds(), sys.argv[1:])
