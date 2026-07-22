from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

TEMPLATE = Path("templates/workspace_results.html")


def _html() -> str:
    return TEMPLATE.read_text(encoding="utf-8", errors="replace")


def test_events_load_all_bypasses_server_side_search_text():
    html = _html()

    assert "async function fetchEventsPage(offset, options = {})" in html
    assert "const q = ignoreSearch ? '' : String(eventsSearch || '').trim();" in html

    assert "async function loadMoreEventsPages(maxPages, options = {})" in html
    assert "const qForKey = ignoreSearch ? '' : String(eventsSearch || '').trim();" in html
    assert "const res = await fetchEventsPage(eventsOffset, { ignoreSearch });" in html

    assert "await loadMoreEventsPages(1, { ignoreSearch: true });" in html
    assert "Events search text. The search box remains a client-side display filter." in html

    # Normal Load more remains filter-aware unless options.ignoreSearch is passed.
    assert "loadMoreEventsPages(1);" in html


def test_events_background_autoload_is_idle_capped_and_non_stuttering():
    html = _html()

    assert "const EVENTS_AUTOLOAD_TOTAL_CAP = 20000;" in html
    assert "function scheduleEventsAutoLoad(options = {})" in html

    assert "one page per idle slice" in html
    assert "eventsBulkLoading = true;" in html
    assert "await loadMoreEventsPages(1, { ignoreSearch });" in html

    assert "refreshFilterOptionsFromEvents();" in html
    assert "if (eventsPanelVisible) {" in html
    assert "renderEventsPreview();" in html
    assert "eventsRenderArmed = true;" in html

    assert "Seamlessly warm the Events cache after results are ready." in html
    assert "scheduleEventsAutoLoad({ ignoreSearch: true, maxTotal: EVENTS_AUTOLOAD_TOTAL_CAP });" in html


def test_events_background_autoload_builds_on_search_safe_load_all():
    html = _html()

    assert "async function loadMoreEventsPages(maxPages, options = {})" in html
    assert "const res = await fetchEventsPage(eventsOffset, { ignoreSearch });" in html
    assert "await loadMoreEventsPages(1, { ignoreSearch: true });" in html
