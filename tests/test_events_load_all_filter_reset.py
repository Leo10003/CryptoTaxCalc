from __future__ import annotations

from pathlib import Path


def test_events_load_all_bypasses_server_side_search_text():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "async function fetchEventsPage(offset, options = {})" in html
    assert "const q = ignoreSearch ? '' : String(eventsSearch || '').trim();" in html

    assert "async function loadMoreEventsPages(maxPages, options = {})" in html
    assert "const qForKey = ignoreSearch ? '' : String(eventsSearch || '').trim();" in html
    assert "const res = await fetchEventsPage(eventsOffset, { ignoreSearch });" in html

    assert "await loadMoreEventsPages(1, { ignoreSearch: true });" in html
    assert "Events search text. The search box remains a client-side display filter." in html

    # Normal Load more remains filter-aware unless options.ignoreSearch is passed.
    assert "loadMoreEventsPages(1);" in html
