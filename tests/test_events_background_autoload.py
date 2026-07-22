from __future__ import annotations

from pathlib import Path


def test_events_background_autoload_is_idle_capped_and_non_stuttering():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

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
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "async function loadMoreEventsPages(maxPages, options = {})" in html
    assert "const res = await fetchEventsPage(eventsOffset, { ignoreSearch });" in html
    assert "await loadMoreEventsPages(1, { ignoreSearch: true });" in html
