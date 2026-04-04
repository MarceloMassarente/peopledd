from __future__ import annotations

from peopledd.vendor.discovery_ranking import (
    authority_score,
    blend_pre_rank_score,
    infer_date_guess,
    interleave_by_source,
    is_structurally_junk,
    sanitize_search_query,
)
from peopledd.vendor.search import SearchResult


def test_sanitize_removes_numbering_preserves_quoted_phrase() -> None:
    assert sanitize_search_query('1. "Itaú Unibanco" relatório anual') == '"Itaú Unibanco" relatório anual'
    assert sanitize_search_query("* item um") == "item um"


def test_interleave_alternates_sources_dedups_url() -> None:
    a = SearchResult(url="https://a.com/p", title="a", score=0.9, source="searxng")
    b = SearchResult(url="https://b.com/p", title="b", score=0.8, source="exa")
    c = SearchResult(url="https://a.com/p", title="dup", score=0.7, source="exa")
    out = interleave_by_source([a, b, c], order=("searxng", "exa"))
    urls = [r.url for r in out]
    assert urls[0] == "https://a.com/p"
    assert urls[1] == "https://b.com/p"
    assert urls.count("https://a.com/p") == 1


def test_authority_gov_and_news() -> None:
    assert authority_score("https://www.gov.br/foo") > authority_score("https://random-blog.example/post")
    assert authority_score("https://g1.globo.com/x") > 0.75
    assert authority_score("https://unknown.example.com/") == 0.5


def test_infer_date_from_url_path() -> None:
    assert infer_date_guess("https://news.example.com/2024/05/01/artigo") == "2024-05-01"


def test_structural_junk_play_store_and_ri_ok() -> None:
    assert is_structurally_junk("https://play.google.com/store/apps/foo")
    assert not is_structurally_junk("https://ri.empresa.com.br/relatorio-anual")


def test_blend_keeps_negative_base() -> None:
    assert blend_pre_rank_score(base_quality=-1.0, authority=1.0, recency=1.0) == -1.0
    blended = blend_pre_rank_score(base_quality=1.0, authority=1.0, recency=1.0)
    assert blended > 1.0
