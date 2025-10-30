import io
from typing import Any, Dict, Iterable, List, Optional

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    """Convert various row-like objects to a plain dict."""
    if isinstance(obj, dict):
        return obj
    m = getattr(obj, "_mapping", None)
    if m is not None:
        try:
            return dict(m)
        except Exception:
            pass
    asdict = getattr(obj, "_asdict", None)
    if callable(asdict):
        try:
            return dict(asdict())
        except Exception:
            pass
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict):
        return {k: v for k, v in d.items() if not k.startswith("_") and k != "_sa_instance_state"}
    return {"value": str(obj)}


def _make_wrapped_table(data: List[List[Any]], styles, page_width_pts: float) -> Table:
    """
    Create a wrapped table that fits the page width.
    - data[0] is the header row.
    - Automatically computes column widths with upper/lower bounds.
    """
    # Paragraph style for wrapping small text
    wrap_style = ParagraphStyle(
        "WrapSmall",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        wordWrap="CJK",
    )

    # Convert cells to Paragraphs (stringify first)
    wrapped: List[List[Paragraph]] = []
    for row in data:
        wrapped.append([Paragraph("" if c is None else str(c), wrap_style) for c in row])

    # Heuristic: column widths based on character length of header + a sample of body rows
    header = [str(c) for c in data[0]] if data else []
    ncols = len(header)

    # Early return trivial case
    if ncols == 0:
        t = Table(wrapped, hAlign="LEFT")
        t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.25, colors.black)]))
        return t

    # Estimate relative weights per column by sampling (header + up to 50 rows)
    def _text_len(cell) -> int:
        s = "" if cell is None else str(cell)
        return max(1, min(len(s), 80))  # cap to avoid over-influence

    samples = wrapped[: min(len(wrapped), 51)]  # header + 50
    weights = [0] * ncols
    for row in samples:
        for i, cell in enumerate(row):
            # Paragraph has a .text property; if not, fall back to length of repr
            txt = getattr(cell, "text", None)
            weights[i] += _text_len(txt if txt is not None else cell)

    # Normalize to widths
    total_w = sum(weights) or ncols
    # Page inner width: leave ~1 inch margins total
    # (SimpleDocTemplate will apply its own margins; we fit to usable width)
    usable_width = page_width_pts - (0.8 * inch)  # be conservative

    # Bounds to keep columns readable
    min_w = 0.7 * inch
    max_w = 1.8 * inch

    # First pass widths
    col_widths = []
    for w in weights:
        frac = (w / total_w) if total_w else (1.0 / ncols)
        col_widths.append(frac * usable_width)

    # Clamp to bounds, then re-scale if needed to fit exactly
    col_widths = [max(min_w, min(max_w, cw)) for cw in col_widths]
    total = sum(col_widths)
    if total > 0:
        scale = usable_width / total
        col_widths = [cw * scale for cw in col_widths]

    t = Table(wrapped, hAlign="LEFT", colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("LEADING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return t


def build_summary_pdf(summary_data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> bytes:
    """
    Generate a PDF summary for the FIFO calculation (backward compatible).

    Accepts keys in summary_data (or as kwargs):
      - title: str
      - year: Optional[int]
      - totals: Dict[str, Any]
      - eur_totals: Dict[str, Any]
      - by_quote: Dict[str, Any]   (kept for compatibility; not rendered here)
      - top_events: Iterable[dict|Row|object]  -> rendered as a table
    """
    if summary_data is None:
        summary_data = {}
    elif not isinstance(summary_data, dict):
        raise TypeError(f"summary_data must be a dict or None, got {type(summary_data).__name__}")

    if kwargs:
        summary_data = {**summary_data, **kwargs}

    title: str = summary_data.get("title", "Crypto Tax – FIFO Summary")
    year: Optional[int] = summary_data.get("year")
    totals: Dict[str, Any] = summary_data.get("totals") or {}
    eur_totals: Dict[str, Any] = summary_data.get("eur_totals") or {}
    top_events_raw: Iterable[Any] = summary_data.get("top_events") or []

    # Normalize top events to list of dicts
    top_events = [_to_plain_dict(ev) for ev in top_events_raw if ev is not None]

    # Choose portrait vs landscape based on column count
    # Prefer a sensible default header order
    preferred = [
        "timestamp",
        "asset",
        "qty_sold",
        "proceeds",
        "cost_basis",
        "gain",
        "quote_asset",
        "fee_applied",
        "lot_id",
        "tx_hash",
    ]
    header: List[str] = []
    if top_events:
        seen = set()
        keys_union: List[str] = []
        for row in top_events:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    keys_union.append(k)
        header = [k for k in preferred if k in seen] + [
            k for k in keys_union if k not in set(preferred)
        ]

    # Select page size
    page_size = A4 if (not header or len(header) <= 8) else landscape(A4)

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    story = []

    # Title / meta
    story.append(Paragraph(title, styles["Title"]))
    if year is not None:
        story.append(Paragraph(f"Tax Year: {year}", styles["Normal"]))
    story.append(Spacer(1, 12))

    # Totals
    if totals:
        story.append(Paragraph("Totals", styles["Heading2"]))
        data = [["Field", "Value"]] + [[k, str(v)] for k, v in totals.items()]
        t = _make_wrapped_table(
            data, styles, page_width_pts=doc.width + doc.leftMargin + doc.rightMargin
        )
        story.append(t)
        story.append(Spacer(1, 10))

    # EUR Totals
    if eur_totals:
        story.append(Paragraph("EUR Totals", styles["Heading2"]))
        data = [["Field", "Value"]] + [[k, str(v)] for k, v in eur_totals.items()]
        t = _make_wrapped_table(
            data, styles, page_width_pts=doc.width + doc.leftMargin + doc.rightMargin
        )
        story.append(t)
        story.append(Spacer(1, 10))

    # Top events table
    if header:
        story.append(Paragraph("Top Realized Events", styles["Heading2"]))
        data = [header]
        for row in top_events:
            data.append([row.get(k, "") for k in header])
        t = _make_wrapped_table(
            data, styles, page_width_pts=doc.width + doc.leftMargin + doc.rightMargin
        )
        story.append(t)
    else:
        # If caller provided no events
        story.append(Paragraph("No events to display.", styles["Normal"]))

    doc.build(story)
    return buffer.getvalue()
