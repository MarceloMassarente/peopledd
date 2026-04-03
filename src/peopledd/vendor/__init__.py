# peopledd vendor package — standalone ports from deepsearch
# All tools are self-contained; no deepsearch repo required.

from peopledd.vendor.scraper import MultiStrategyScraper, ScraperConfig, ScrapeResult
from peopledd.vendor.search import (
    SearchOrchestrator,
    SearchPlanner,
    ExaProvider,
    SearXNGProvider,
    URLSelector,
    CompanyProfile,
    SearchResult,
)
from peopledd.vendor.document_store import InMemoryDocumentStore, DocumentCache

__all__ = [
    # scraper
    "MultiStrategyScraper",
    "ScraperConfig",
    "ScrapeResult",
    # search
    "SearchOrchestrator",
    "SearchPlanner",
    "ExaProvider",
    "SearXNGProvider",
    "URLSelector",
    "CompanyProfile",
    "SearchResult",
    # document store
    "InMemoryDocumentStore",
    "DocumentCache",
]
