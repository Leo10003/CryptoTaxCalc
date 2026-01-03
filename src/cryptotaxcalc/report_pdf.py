# report_pdf.py – Optimized and Auditable
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Optional
import hashlib

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)

from cryptotaxcalc.logging_setup import get_logger, _atomic_write_json, _now_iso_z

try:
    import matplotlib as mpl  # type: ignore
    mpl.use("Agg")  # non-interactive backend suitable for PDFs/PNGs
    import matplotlib.pyplot as _plt  # type: ignore
except Exception:  # matplotlib is optional; gracefully degrade if missing
    _plt = None

# ---- Matplotlib font fix ----
if _plt is not None:
    try:
        font_path = "static/fonts/LiberationSans-Regular.ttf"
        mpl.font_manager.fontManager.addfont(font_path)
        mpl.rcParams["font.family"] = "Liberation Sans"
        mpl.rcParams["font.size"] = 9
        mpl.rcParams["axes.titlesize"] = 9
        mpl.rcParams["axes.labelsize"] = 8
        mpl.rcParams["legend.fontsize"] = 8
    except Exception:
        # silently continue — fallback to default if font missing
        pass

logger = get_logger("pdf")

# Global visual tuning constants for consistent layout
HEADER_BG = colors.Color(0.94, 0.95, 0.98)
ACCENT_BG = colors.Color(0.30, 0.60, 0.95)

# Secondary accent for compliance / methodology / country notes sections
COMPLIANCE_HEADER_BG = colors.Color(0.93, 0.93, 0.95)
COMPLIANCE_ACCENT_BG = colors.Color(0.35, 0.40, 0.52)

CARD_BG = colors.Color(0.97, 0.97, 0.99)
CARD_BORDER_COLOR = colors.lightgrey

# Vertical rhythm (Option A – Soft Financial)
# Slightly increased to give a more "premium" breathing rhythm across sections.
SECTION_SPACING = 18              # space between major sections (was 16)
SUBSECTION_SPACING = SECTION_SPACING // 2  # 9pt: header → content spacing
MICRO_SPACING = 6                 # tiny separators for notes, bullets, etc.

# Header icons tuned a bit larger for legibility and perceived polish.
ICON_SIZE = 28       # header icon size in points

# ----------------------------
# Utilities & safe conversions
# ----------------------------

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v)

def _to_float(v: Any) -> float | None:
    """Best-effort float coercion for numeric formatting."""
    if v is None or v == "":
        return None
    try:
        return float(str(v))
    except (TypeError, ValueError):
        return None

def _fmt_eur(v: Any) -> str:
    """Format a value as EUR, e.g. € 1,234.56."""
    x = _to_float(v)
    if x is None:
        return ""
    return f"€ {x:,.2f}"

def _fmt_signed_eur(v: Any) -> str:
    """Format a value as signed EUR, e.g. +€ 123.45 / -€ 123.45."""
    x = _to_float(v)
    if x is None:
        return ""
    sign = "+" if x > 0 else ("-" if x < 0 else "")
    return f"{sign}€ {abs(x):,.2f}"

def _ensure_rows_same_length(rows: List[Sequence[Any]]) -> List[List[Any]]:
    """Normalize a list of rows to same number of columns."""
    if not rows:
        return [["(no data)"]]
    ncols = len(rows[0])
    out: List[List[Any]] = []
    for r in rows:
        rr = list(r)
        if len(rr) < ncols:
            rr += [""] * (ncols - len(rr))
        elif len(rr) > ncols:
            rr = rr[:ncols]
        out.append(rr)
    return out

def _mk_table(
    data: List[Sequence[Any]],
    col_widths: Sequence[Any] | None = None,
    style: TableStyle | None = None,
) -> Table:
    """Build a robust ReportLab Table with auto fallback widths."""
    normalized = _ensure_rows_same_length(data)
    ncols = len(normalized[0])
    table = Table(normalized, colWidths=list(col_widths)) if col_widths and len(col_widths) == ncols else Table(normalized)
    if style is None:
        style = TableStyle([
            # Header row
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("TOPPADDING", (0, 0), (-1, 0), 6),

            # Body cells
            ("LEFTPADDING", (0, 1), (-1, -1), 4),
            ("RIGHTPADDING", (0, 1), (-1, -1), 4),
            ("TOPPADDING", (0, 1), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),

            # Right-align body numeric columns (all except first column)
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),

            # Soft grid and zebra striping
            ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.Color(0.98, 0.98, 0.98), colors.white]),
        ])
    table.setStyle(style)
    return table

def _find_logo_path(explicit_path: str | None = None) -> Path | None:
    """Resolve the logo path, checking explicit, local ./logo, and repo-level paths."""
    candidates: List[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.append(Path.cwd() / "logo" / "logo.png")
    here = Path(__file__).resolve()
    for p in [here.parent, here.parent.parent, here.parent.parent.parent]:
        candidates.append(p / "logo" / "logo.png")
    for c in candidates:
        if c.is_file():
            return c
    return None

def _find_icon_path(filename: str) -> Path | None:
    """
    Resolve an icon path for section headers.
    Looks under ./static/img/icons and up the tree for repo-level static.
    """
    if not filename:
        return None

    candidates: List[Path] = []
    # CWD-relative (project root when running uvicorn in dev)
    candidates.append(Path("static") / "img" / "icons" / filename)

    # Relative to this file, walking up a few levels
    here = Path(__file__).resolve()
    for p in [here.parent, here.parent.parent, here.parent.parent.parent]:
        candidates.append(p / "static" / "img" / "icons" / filename)

    for c in candidates:
        if c.is_file():
            return c

    logger.debug(f"Icon not found for PDF header: {filename}")
    return None


def _icon_flowable(filename: str | None, size: float = 24) -> Image | None:
    """Return a small Image flowable for a header icon, or None if missing."""
    if not filename:
        return None
    path = _find_icon_path(filename)
    if not path:
        return None
    try:
        img = Image(str(path), width=size, height=size)
        img.hAlign = "LEFT"
        return img
    except Exception as e:
        logger.warning(f"Failed to load icon {filename}: {e}")
        return None


# ----------------------
# Document/style helpers
# ----------------------

_styles_cache: Dict[str, Any] | None = None

def _styles():
    global _styles_cache
    if _styles_cache is not None:
        return _styles_cache

    s = getSampleStyleSheet()

    # Title and subtitle tuned for a more "premium" visual hierarchy.
    title = ParagraphStyle(
        "TitleBig",
        parent=s["Title"],
        fontSize=23,   # slightly larger for stronger first impression
        leading=28,
        spaceAfter=10,
    )
    subtitle = ParagraphStyle(
        "Subtitle",
        parent=s["Normal"],
        fontSize=10,
        leading=13,    # more line spacing = easier scanning of long subtitle
        textColor=colors.grey,
        spaceAfter=14,
    )

    # Section headers: slightly softer size, tighter vertical padding so the bar
    # feels dense but not cramped.
    h2 = ParagraphStyle(
        "H2",
        parent=s["Heading2"],
        fontSize=15,   # was 16 – small reduction keeps icons visually dominant
        leading=19,
        spaceBefore=8,
        spaceAfter=4,
    )

    # Table and body text: subtle leading boost for calmer reading.
    cell = ParagraphStyle("TableCell", parent=s["Normal"], fontSize=9, leading=12)
    note = ParagraphStyle("Note", parent=s["Italic"], fontSize=8, textColor=colors.grey)
    body = ParagraphStyle("BodyText", parent=s["Normal"], fontSize=9, leading=13)  # was 12

    _styles_cache = {
        "TitleBig": title,
        "Subtitle": subtitle,
        "H2": h2,
        "TableCell": cell,
        "Note": note,
        "BodyText": body,
        "Normal": s["Normal"],
        "Title": s["Title"],
        "Heading2": s["Heading2"],
        "Italic": s["Italic"],
    }
    return _styles_cache

class NumberedCanvas(rl_canvas.Canvas):
    """
    Canvas that knows total page count and draws the footer itself.

    Pattern:
      1. During the build, we just save each page state and call _startPage().
      2. In save(), we replay the pages once, now knowing the total page count,
         and draw the footer for each page.
    """
    def __init__(self, *args, run_id: str = "", generated_ts: str = "", **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states: list[dict[str, Any]] = []
        self._run_id = run_id or ""
        self._generated_ts = generated_ts or ""

    def showPage(self):
        # Store current page state; do NOT write the page out yet.
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()  # prepare for the next page

    def draw_footer(self, page_count: int):
        """Draw standard footer: disclaimer + generated + run id + Page X of Y."""
        footer_y = 0.5 * inch
        self.saveState()

        self.setFont("Helvetica", 7)
        self.setFillColor(colors.grey)

        disclaimer = (
            "These results are for information only — not tax advice. "
            "Your actual obligations depend on your full situation and local rules."
        )
        # Center disclaimer just above the footer line
        self.drawCentredString(self._pagesize[0] / 2.0, footer_y + 10, disclaimer)

        # Left side: generated timestamp and run id
        x_left = self._pagesize[0] * 0.08
        if self._generated_ts:
            self.drawString(x_left, footer_y, f"Generated: {self._generated_ts}")
        if self._run_id:
            self.drawString(x_left, footer_y - 9, f"Run ID: {self._run_id}")

        # Right side: Page X of Y
        page_num = self.getPageNumber()
        page_label = f"Page {page_num} of {page_count}"
        x_right = self._pagesize[0] - self._pagesize[0] * 0.08
        self.drawRightString(x_right, footer_y, page_label)

        self.restoreState()

    def save(self):
        page_count = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_footer(page_count)
            rl_canvas.Canvas.showPage(self)
        rl_canvas.Canvas.save(self)


def _page_template(doc: BaseDocTemplate) -> PageTemplate:
    return PageTemplate(
        id="main",
        frames=[Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height)],
    )

def _logo_flowable(payload: Dict[str, Any], doc: BaseDocTemplate) -> Image | None:
    """Return a scaled Image flowable if logo exists; otherwise None."""
    logo_path = _find_logo_path(_safe_str(payload.get("logo_path") or None))
    if not logo_path:
        logger.debug("No logo found for PDF header.")
        return None
    try:
        img = Image(str(logo_path))
        max_w = min(3 * inch, doc.width)
        max_h = 1.5 * inch
        img.hAlign = "RIGHT"
        img._restrictSize(max_w, max_h)
        return img
    except Exception as e:
        logger.warning(f"Failed to load logo: {e}")
        return None

def _extract_eur_totals(payload: dict):
    """
    Returns (totals_dict, notes_list) from payload regardless of whether
    eur_summary is top-level, nested under summary, or even deeper.
    """
    root = None
    # try direct
    if isinstance(payload.get("eur_summary"), dict):
        root = payload["eur_summary"]
    # try under summary
    elif isinstance(payload.get("summary"), dict) and isinstance(payload["summary"].get("eur_summary"), dict):
        root = payload["summary"]["eur_summary"]
    # try deeper under summary > summary
    elif isinstance(payload.get("summary"), dict):
        sub = payload["summary"].get("summary")
        if isinstance(sub, dict) and isinstance(sub.get("eur_summary"), dict):
            root = sub["eur_summary"]

    if not root:
        return {}, []

    totals = root.get("totals_eur", {}) if isinstance(root, dict) else {}
    notes = root.get("notes", []) if isinstance(root, dict) else []
    return totals, notes

def _build_bar_chart_image(
    labels: List[str],
    values: List[float],
    title: str,
    width: float = 5.2 * inch,
    height: float = 3.0 * inch,
):
    """
    Build a compact horizontal bar chart as a ReportLab Image.
    If matplotlib is unavailable or no data, returns None.
    """
    if _plt is None:
        return None

    # Filter out None values and keep pairs together
    pairs = [(lab, val) for lab, val in zip(labels, values) if val is not None]
    if not pairs:
        return None

    # Sort by absolute value so the biggest contributors stand out visually
    pairs.sort(key=lambda t: abs(t[1]), reverse=True)
    labels_sorted = [p[0] for p in pairs]
    values_sorted = [float(t[1]) for t in pairs]

    fig_w_in = width / 72.0
    fig_h_in = height / 72.0

    fig, ax = _plt.subplots(figsize=(fig_w_in, fig_h_in))

    y_pos = range(len(labels_sorted))
    ax.barh(y_pos, values_sorted)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels_sorted)
    ax.invert_yaxis()  # top asset at the top
    ax.set_title(title, fontsize=9)
    
    def _compact_num(x: float) -> str:
        axv = abs(float(x))
        if axv >= 1_000_000:
            return f"{x / 1_000_000:.1f}M"
        if axv >= 1_000:
            return f"{x / 1_000:.0f}k"
        return f"{x:.0f}"

    # Add compact numeric labels at the end of each bar (e.g. "2.6M", "-450k")
    max_val = max(abs(v) for v in values_sorted) if values_sorted else 0
    if max_val > 0:
        offset = max_val * 0.01
        for i, v in enumerate(values_sorted):
            x_pos = (v + offset) if v >= 0 else (v - offset)
            ha = "left" if v >= 0 else "right"
            ax.text(
                x_pos,
                i,
                _compact_num(v),
                va="center",
                ha=ha,
                fontsize=7,
            )

    # Minimal, finance-style look
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis="x", linestyle="--", linewidth=0.4)
    ax.tick_params(axis="y", labelsize=8, pad=4)
    ax.tick_params(axis="x", labelsize=8)

    # Avoid scientific notation like "1e6" on finance charts
    try:
        from matplotlib.ticker import FuncFormatter
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: _compact_num(x)))
        ax.get_xaxis().get_offset_text().set_visible(False)
    except Exception:
        pass

    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110)
    _plt.close(fig)
    buf.seek(0)

    img = Image(buf)
    img._restrictSize(width, height)
    return img

def _build_pie_chart_image(
    labels: List[str],
    values: List[float],
    title: str,
    width: float = 5.2 * inch,
    height: float = 3.2 * inch,
    keep_zeros: bool = False,
    legend_title: str = "Asset",
):
    """
    Build a portfolio composition / split pie chart as a ReportLab Image.

    - Uses a legend instead of labels on the slices to avoid overlap.
    - Only shows percentage labels for slices above a minimum threshold.
    - keep_zeros=True keeps 0-value categories in the legend (useful for Taxable vs Exempt).
    """
    if _plt is None:
        return None

    pairs = []
    for lab, val in zip(labels, values):
        if val is None:
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if v == 0 and not keep_zeros:
            continue
        # pie wedge sizes must be non-negative; we use abs for safety
        pairs.append((lab, abs(v)))

    if not pairs:
        return None

    pairs.sort(key=lambda t: abs(t[1]), reverse=True)
    labels_sorted = [p[0] for p in pairs]
    values_sorted = [float(t[1]) for t in pairs]

    if sum(values_sorted) <= 0:
        return None

    fig_w_in = width / 72.0
    fig_h_in = height / 72.0
    fig, ax = _plt.subplots(figsize=(fig_w_in, fig_h_in))

    def _autopct(pct: float) -> str:
        return f"{pct:.0f}%" if pct >= 4 else ""

    wedges, _texts, _autotexts = ax.pie(
        values_sorted,
        labels=None,
        autopct=_autopct,
        startangle=90,
        textprops={"fontsize": 7},
    )

    ax.set_title(title, fontsize=9)
    ax.axis("equal")

    ax.legend(
        wedges,
        labels_sorted,
        title=str(legend_title or ""),
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=8,
        title_fontsize=9,
        labelspacing=0.8,
        handletextpad=1.0,
    )

    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110)
    _plt.close(fig)
    buf.seek(0)

    img = Image(buf)
    img._restrictSize(width, height)
    return img


def _build_line_chart_image(
    labels: List[str],
    values: List[float],
    title: str,
    width: float = 5.2 * inch,
    height: float = 2.4 * inch,
):
    """
    Build a simple line chart (e.g. cumulative gain over time).
    X-axis uses provided labels as tick labels.
    """
    if _plt is None:
        return None

    if not labels or not values or len(labels) != len(values):
        return None

    fig_w_in = width / 72.0
    fig_h_in = height / 72.0
    fig, ax = _plt.subplots(figsize=(fig_w_in, fig_h_in))

    x = list(range(len(labels)))
    ax.plot(x, values, marker="o", linewidth=1.3)
    ax.set_title(title, fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
    ax.tick_params(axis="y", labelsize=8)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", linewidth=0.4)

    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110)
    _plt.close(fig)
    buf.seek(0)

    img = Image(buf)
    img._restrictSize(width, height)
    return img

# -----------------------
# Main PDF builder entry
# -----------------------

def _brand_header(canvas, doc):
    """Premium brand header for page 1 only."""
    canvas.saveState()
    width = doc.pagesize[0]

    # Soft brand bar
    canvas.setFillColor(colors.Color(0.94, 0.95, 0.98))  # match HEADER_BG
    canvas.rect(0, doc.pagesize[1] - 70, width, 70, fill=1, stroke=0)

    # Title text color
    canvas.setFillColor(colors.Color(0.10, 0.12, 0.16))
    canvas.setFont("Helvetica-Bold", 16)
    canvas.drawString(doc.leftMargin, doc.pagesize[1] - 45, "CryptoTaxCalc – Summary Report")

    canvas.restoreState()


def build_summary_pdf(payload: Dict[str, Any]) -> bytes:
    """
    Build a summary PDF from payload and return its bytes.
    Automatically logs diagnostics to logs/pdf/last_run.json.
    """
    start_ts = _now_iso_z()
    run_id_val = _safe_str(payload.get("run_id")) if payload.get("run_id") not in (None, "") else None
    title_text = _safe_str(payload.get("title") or "Crypto Tax Summary")
    
    # Detect whether this is a demo run (controls synthetic fallback data)
    title_lower = title_text.lower()
    is_demo_run = bool(
        payload.get("is_demo")
        or payload.get("demo")
        or payload.get("demo_mode")
        or "demo" in title_lower
    )
    
    # Normalised jurisdiction code used across sections (cover, tax, country notes)
    juris_raw = payload.get("jurisdiction") or ""
    juris_norm = str(juris_raw).strip().lower()

    logger.info(f"Building summary PDF for run_id={run_id_val or 'N/A'} | title={title_text}")
    
    # Feature toggles and layout configuration (can be driven by payload)
    show_yearly_tax_block = bool(payload.get("show_yearly_tax_block", True))
    show_events_table = bool(payload.get("show_events_table", True))
    show_data_quality_block = bool(payload.get("show_data_quality_block", True))
    show_portfolio_charts = bool(payload.get("show_portfolio_charts", True))
    show_timeline_chart = bool(payload.get("show_timeline_chart", True))
    show_tax_helpers = bool(payload.get("show_tax_helpers", True))
    show_audit_appendix = bool(payload.get("show_audit_appendix", True))
    max_event_rows = int(payload.get("max_event_rows", 100) or 100)

    buf = BytesIO()
    doc = BaseDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=0.8 * inch,
        rightMargin=0.8 * inch,
        topMargin=0.9 * inch,
        bottomMargin=0.9 * inch,
        title=title_text,
        author="CryptoTaxCalc",
        subject="Automated Tax Summary Report",
    )
    # Create main template once so we can refer to its id
    main_tpl = _page_template(doc)

    # Cover template: brand header only; footer is drawn by NumberedCanvas
    cover_tpl = PageTemplate(
        id="cover",
        frames=[Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height)],
        onPage=_brand_header,
    )
    cover_tpl.autoNextPageTemplate = "main"  # <-- FIXED

    doc.addPageTemplates([
        cover_tpl,
        main_tpl,
    ])
    styles = _styles()
    story: List[Any] = []
    
    def add_section_header(text: str, lead_in: str | None = None) -> None:
        """Card-style section header bar with a subtle accent strip, optional icon, and micro lead-in."""
        # Map leading emoji to icon filenames
        emoji_to_icon = {
            "📊": "executive_summary.png",
            "💡": "insights.png",
            "🛡": "scope_safety.png",
            "🛡️": "scope_safety.png",
            "📈": "visual_snapshot.png",
            "💶": "eur_summary.png",
            "💱": "summary_by_quote_asset.png",
            "💰": "Yearly_Tax_Position.png",
            "📜": "events.png",
            "🧪": "data_quality.png",
            "🧾": "methodology.png",
            "🌍": "country_notes.png",
            "📅":"summary_by_month.png",
            "🪙":"summary_by_asset.png",
            "📁":"contents.png",
        }

        icon_filename: str | None = None
        clean_text = text

        # Decide accent color based on section type (data vs compliance)
        compliance_emojis = {"🛡", "🛡️", "🧾", "🌍", "🧪",}
        data_emojis = {"📊", "💡", "📈", "💶", "💱", "💰", "📜", "📁", "📅"}

        leading_emoji = text[0] if text else ""
        if leading_emoji in compliance_emojis:
            header_bg = COMPLIANCE_HEADER_BG
            accent_bg = COMPLIANCE_ACCENT_BG
        else:
            header_bg = HEADER_BG
            accent_bg = ACCENT_BG

        # If the first character is one of our emojis, strip it and attach the icon.
        # Also strip the optional variation selector (️) that can follow some emojis.
        if text:
            first = text[0]
            if first in emoji_to_icon:
                icon_filename = emoji_to_icon[first]
                clean_text = text[1:]
                clean_text = clean_text.lstrip("️ ")  # remove VS16 and any spaces

        heading = Paragraph(clean_text, styles["H2"])
        icon_flow = _icon_flowable(icon_filename, size=ICON_SIZE) if icon_filename else None

        if icon_flow is not None:
            # [accent strip] [icon] [heading]
            data = [["", icon_flow, heading]]
            col_widths = [4, 28, doc.width - 32]
        else:
            data = [["", heading]]
            col_widths = [4, doc.width - 4]

        bar = Table(
            data,
            colWidths=col_widths,
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), header_bg),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),

                # general padding
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),

                # accent strip in the leftmost column
                ("BACKGROUND", (0, 0), (0, 0), accent_bg),

                # icon column padding
                ("LEFTPADDING", (1, 0), (1, 0), 6),
                ("RIGHTPADDING", (1, 0), (1, 0), 4),

                # heading column padding
                ("LEFTPADDING", (2, 0), (2, 0), 4),
                ("RIGHTPADDING", (2, 0), (2, 0), 6),

                ("ALIGN", (1, 0), (1, 0), "LEFT"),
                ("ALIGN", (2, 0), (2, 0), "LEFT"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ]),
        )
        story.append(bar)
        story.append(Spacer(1, SECTION_SPACING // 2))

        # Micro lead-in line (soft gray, small, just under the header)
        if lead_in:
            story.append(
                Paragraph(
                    f"<font size='8' color='#666666'>{lead_in}</font>",
                    styles['BodyText'],
                )
            )
            story.append(Spacer(1, 6))
        

    try:
        # ---- Cover block (premium front page) ----
        cover_logo = _logo_flowable(payload, doc)
        cover_title = Paragraph("Crypto Tax – FIFO Summary", styles["TitleBig"])
        if is_demo_run:
            subtitle_text = (
                "Automated FIFO tax summary – synthetic demo data tuned for Croatia and Italy."
            )
            scope_text = (
                "Demo scope: this PDF uses synthetic demo trades for illustration only and is currently "
                "tuned for Croatia and Italy."
            )
        else:
            subtitle_text = (
                "Automated FIFO capital gains summary for the selected taxpayer and tax year."
            )
            scope_text = (
                "This report summarises transaction data for the selected taxpayer and tax year using the "
                "configured CryptoTaxCalc rules."
            )

        cover_subtitle = Paragraph(subtitle_text, styles["Subtitle"])

        # Title + logo layout
        if cover_logo:
            cover_table = _mk_table(
                [[cover_title, cover_logo]],
                col_widths=[doc.width * 0.65, doc.width * 0.35],
            )
        else:
            cover_table = _mk_table([[cover_title]], col_widths=[doc.width])

        # --- Premium cover card ---
        cover_card_rows = [
            [cover_title],
            [Spacer(1, 6)],
            [cover_subtitle],
        ]

        if cover_logo:
            cover_card_rows.insert(0, [cover_logo])

        cover_card = Table(
            cover_card_rows,
            colWidths=[doc.width],
            style=TableStyle([
                ("BACKGROUND", (0,0), (-1,-1), CARD_BG),
                ("BOX", (0,0), (-1,-1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0,0), (-1,-1), 16),
                ("RIGHTPADDING", (0,0), (-1,-1), 16),
                ("TOPPADDING", (0,0), (-1,-1), 18),
                ("BOTTOMPADDING", (0,0), (-1,-1), 16),
            ]),
        )

        story.append(Spacer(1, 0.9 * inch))
        story.append(cover_card)
        story.append(Spacer(1, 0.4 * inch))

        # Currency note (explicit EUR context)
        currency_note = Paragraph(
            "All monetary amounts are shown in EUR unless stated otherwise.",
            styles["Note"],
        )
        story.append(currency_note)
        story.append(Spacer(1, 0.35 * inch))

        demo_scope_note = Paragraph(scope_text, styles["Note"])
        story.append(demo_scope_note)
        story.append(Spacer(1, 0.25 * inch))

        # Compact metadata lines on the cover
        meta_lines = []

        # Jurisdiction
        if juris_raw:
            meta_lines.append(f"Jurisdiction: {_safe_str(juris_raw)}")
            
            # Jurisdiction readiness (disclosure hook for newly added/placeholder jurisdictions)
            if juris_norm and juris_norm not in {"hr", "it"}:
                if juris_norm == "xx":
                    meta_lines.append("Jurisdiction readiness: Baseline (testing / onboarding)")
                else:
                    meta_lines.append("Jurisdiction readiness: Baseline (generic notes only)")

        # Taxpayer identity (optional; demo-safe fallback)
        taxpayer_name = _safe_str(payload.get("taxpayer_name")) if payload.get("taxpayer_name") else ""
        taxpayer_id = _safe_str(payload.get("taxpayer_id")) if payload.get("taxpayer_id") else ""

        if is_demo_run:
            if not taxpayer_name:
                taxpayer_name = "Demo taxpayer"
            if not taxpayer_id:
                # Jurisdiction-specific demo IDs for illustration
                if juris_norm in {"hr", "croatia", "croat"}:
                    taxpayer_id = "DEMO-OIB-00000000000"
                elif juris_norm in {"it", "italy", "ital"}:
                    taxpayer_id = "DEMO-CF-ABCDEF12G34H567I"
                else:
                    taxpayer_id = "DEMO-TAXID-000000"
        # For non-demo runs we only show values if supplied; no placeholders

        if taxpayer_name:
            meta_lines.append(f"Taxpayer: {taxpayer_name}")

        if taxpayer_id:
            if juris_norm in {"hr", "croatia", "croat"}:
                id_label = "OIB"
            elif juris_norm in {"it", "italy", "italia"}:
                id_label = "Tax ID (Codice fiscale / P. IVA)"
            else:
                id_label = "Tax ID"
            meta_lines.append(f"{id_label}: {taxpayer_id}")

        # Configuration metadata
        rule_version = payload.get("rule_version")
        if rule_version:
            meta_lines.append(f"Rule version: {_safe_str(rule_version)}")

        # Scope metadata (prevents confusion between configured tax_year vs filtered scope)
        scope_asset = _safe_str(payload.get("scope_asset") or "")
        scope_year = _safe_str(payload.get("scope_year") or "")
        if scope_asset or scope_year:
            meta_lines.append(f"Scope: {scope_asset or 'All assets'} • {scope_year or 'All years'}")

        period_start = _safe_str(payload.get("period_start") or "")
        period_end = _safe_str(payload.get("period_end") or "")
        if period_start and period_end:
            meta_lines.append(f"Period: {period_start} — {period_end}")

        tax_year = payload.get("tax_year")
        if tax_year:
            # If the PDF is explicitly scoped, label run.tax_year as configuration (not scope).
            if scope_year and scope_year != _safe_str(tax_year):
                meta_lines.append(f"Configured tax year: {_safe_str(tax_year)}")
            else:
                meta_lines.append(f"Tax year: {_safe_str(tax_year)}")

        gen_at = payload.get("generated_at")
        if gen_at:
            meta_lines.append(f"Generated at: {_safe_str(gen_at)}")

        if run_id_val:
            meta_lines.append(f"Run ID: {_safe_str(run_id_val)}")

        if meta_lines:
            meta_table = Table(
                [[Paragraph(line, styles["BodyText"])] for line in meta_lines],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0,0), (-1,-1), colors.Color(0.97,0.97,0.99)),
                    ("BOX", (0,0), (-1,-1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0,0), (-1,-1), 10),
                    ("RIGHTPADDING", (0,0), (-1,-1), 10),
                    ("TOPPADDING", (0,0), (-1,-1), 8),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 8),
                ])
            )
            story.append(meta_table)
            story.append(Spacer(1, 0.45 * inch))
        
        # Premium bullet styling for the cover overview (emoji are replaced with reliable ReportLab glyphs)
        scope_bullet = (
            "<b><font color='#4B9BFF'>●</font> Scope & Safety</b> – what this demo run covers and how to interpret it safely."
            if is_demo_run
            else "<b><font color='#4B9BFF'>●</font> Scope & Safety</b> – what this report covers and how to interpret it safely."
        )

        eur_totals_preview, _eur_notes_preview = _extract_eur_totals(payload)

        gain_preview = 0.0
        try:
            gain_preview = float(eur_totals_preview.get("gain", 0) or 0)
        except Exception:
            gain_preview = 0.0

        # Prefer taxable gain when deciding whether a tax illustration is meaningful.
        rt_preview = payload.get("run_totals") or {}
        taxable_preview = _to_float(rt_preview.get("taxable_gain_eur") or rt_preview.get("taxable_gain"))
        if taxable_preview is None:
            taxable_preview = gain_preview

        will_show_tax_illustration = bool(show_yearly_tax_block and taxable_preview > 0)

        eur_bullet = (
            "<b><font color='#4B9BFF'>●</font> EUR Summary & tax illustration</b> – consolidated EUR totals and one example yearly tax view."
            if will_show_tax_illustration
            else "<b><font color='#4B9BFF'>●</font> EUR Summary</b> – consolidated EUR totals for this report."
        )

        overview_points = [
            "<b><font color='#4B9BFF'>●</font> Executive Summary</b> – your key EUR totals and event count in one glance.",
            scope_bullet,
            "<b><font color='#4B9BFF'>●</font> Summary tables</b> – breakdown by month, quote asset and underlying asset.",
            "<b><font color='#4B9BFF'>●</font> Visual Snapshot</b> – charts that make the main patterns easier to see.",
            eur_bullet,
            "<b><font color='#4B9BFF'>●</font> Events (sample)</b> – deterministic sample across the scope (includes top gains/losses and time coverage).",
            "<b><font color='#4B9BFF'>●</font> Data quality, glossary & methodology</b> – checks, definitions and the technical rules used.",
        ]

        overview_rows = [
            [Paragraph(txt, styles["BodyText"])]
            for txt in overview_points
        ]

        overview_card = Table(
            overview_rows,
            colWidths=[doc.width],
            style=TableStyle([
                ("BACKGROUND", (0,0), (-1,-1), CARD_BG),
                ("BOX", (0,0), (-1,-1), 0.25, CARD_BORDER_COLOR),
                ("TOPPADDING", (0,0), (-1,-1), 10),
                ("BOTTOMPADDING", (0,0), (-1,-1), 10),
                ("LEFTPADDING", (0,0), (-1,-1), 14),
                ("RIGHTPADDING", (0,0), (-1,-1), 14),
                ("ROWSPACING", (0,0), (-1,-1), 4),
            ])
        )

        # End of cover page
        story.append(PageBreak())

        # --- Contents page ---
        add_section_header(
            "📁 Contents",
            "Sections included in this report. Use this page to jump to the part your reviewer cares about most."
        )
        story.append(overview_card)
        story.append(Spacer(1, SECTION_SPACING))

        # Start Executive Summary on its own page
        story.append(PageBreak())

        # ---- Executive Summary (Ultra-Premium block) ----
        # Extract EUR totals robustly
        eur_totals, eur_notes = _extract_eur_totals(payload)
        proceeds_eur = eur_totals.get("proceeds", 0)
        cost_eur = eur_totals.get("cost_basis", 0)
        gain_eur = eur_totals.get("gain", 0)

        # Events: prefer full-count (events_count_total); fallback to sample length
        events_sample = len(payload.get("top_events") or [])
        total_events = events_sample
        try:
            v = payload.get("events_count_total")
            if v is not None:
                total_events = int(v)
        except Exception:
            total_events = events_sample

        # Build executive summary table
        exec_rows = [
            ["Proceeds (EUR)", _fmt_eur(proceeds_eur)],
            ["Cost Basis (EUR)", _fmt_eur(cost_eur)],
            ["Net Gain (EUR)", _fmt_signed_eur(gain_eur)],
            ["Events (total)", str(total_events)],
        ]
        
        if events_sample != total_events:
            exec_rows.append(["Events shown in PDF", str(events_sample)])

        # Jurisdiction if available
        juris = payload.get("jurisdiction")
        if juris:
            exec_rows.append(["Jurisdiction", _safe_str(juris)])

        # Timestamp
        gen = payload.get("generated_at")
        if gen:
            exec_rows.append(["Generated", _safe_str(gen)])

        add_section_header(
            "📊 Executive Summary",
            "High-level snapshot of your taxable performance for this run, optimised for quick reading."
        )
        story.append(Spacer(1, SUBSECTION_SPACING))

        # Optional soft shading background
        exec_table_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.95, 0.95, 0.97)),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            # Emphasise Net Gain row (index 2)
            ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
            ("FONTSIZE", (0, 2), (-1, 2), 11),
        ])
        
        inner_exec = _mk_table(exec_rows, style=exec_table_style)
        exec_card = Table(
            [[inner_exec]],
            colWidths=[doc.width],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        )
        story.append(exec_card)
        story.append(Spacer(1, 4))
        story.append(
            Paragraph(
                "<font size='8' color='#666666'>Net gain (EUR) is the key headline figure this summary highlights for this run.</font>",
                styles["Normal"],
            )
        )
        story.append(Spacer(1, SECTION_SPACING))
        
        # ---- Insights & commentary ----
        insights: List[str] = []

        # Coerce gain to numeric for comparisons
        gain_numeric = _to_float(gain_eur)
        if gain_numeric is not None:
            if gain_numeric > 0:
                insights.append("This run shows a <b>net taxable gain</b> in EUR.")
            elif gain_numeric < 0:
                insights.append("This run shows a <b>net taxable loss</b> in EUR.")
            else:
                insights.append("This run is approximately <b>break-even</b> in EUR.")

        if isinstance(summary_by_asset := payload.get("summary_by_asset"), dict) and summary_by_asset:
            top_asset = max(summary_by_asset.items(), key=lambda kv: abs(_to_float(kv[1].get("gain")) or 0.0))
            asset_name, asset_vals = top_asset
            top_gain = _to_float(asset_vals.get("gain")) or 0.0
            if top_gain != 0.0:
                direction = "gain" if top_gain > 0 else "loss"
                insights.append(f"<b>{_safe_str(asset_name)}</b> is the largest contributor to realized {direction} in this run.")

        if isinstance(summary_by_quote := payload.get("summary_by_quote"), dict) and summary_by_quote:
            top_quote = max(summary_by_quote.items(), key=lambda kv: abs(_to_float(kv[1].get("proceeds")) or 0.0))
            quote_name, quote_vals = top_quote
            proceeds_val = _to_float(quote_vals.get("proceeds")) or 0.0
            if proceeds_val > 0:
                insights.append(f"<b>{_safe_str(quote_name)}</b> is the dominant quote asset by proceeds for this run.")

        if total_events:
            dataset_label = "demo dataset" if is_demo_run else "dataset"
            insights.append(
                f"This summary is based on <b>{total_events}</b> realised events in the {dataset_label}."
            )

        if insights:
            # Keep insights on the same page as the executive summary
            story.append(Spacer(1, SECTION_SPACING))
            add_section_header(
                "💡 Insights & Commentary",
                "Auto-generated commentary that highlights the most relevant drivers in this run."
            )
            bullet_rows = [[Paragraph("• " + line, styles["BodyText"])] for line in insights]
            inner_insights = _mk_table(bullet_rows)
            insights_card = Table(
                [[inner_insights]],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                    ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]),
            )
            story.append(insights_card)
            story.append(Spacer(1, SECTION_SPACING))

        
        # ---- Scope & Safety Section (Premium cards) ----
        story.append(Spacer(1, SECTION_SPACING))
        scope_lead = (
            "What this demo report covers, how calculations are performed, and how it should be interpreted."
            if is_demo_run
            else "What this report covers, how calculations are performed, and how it should be interpreted."
        )
        add_section_header("🛡️ Scope & Safety", scope_lead)

        if is_demo_run:
            notes_left = [
                "End-to-end pipeline: Import → Normalize → Calculate → Export.",
                "FIFO with FX normalisation applied to every taxable disposal.",
                "All numbers are produced from a synthetic demo dataset.",
                "Each demo run includes an internal diagnostics bundle.",
            ]
            notes_right = [
                "This report is for illustration and training only — it is not tax advice.",
                "Results may differ from a real client configuration.",
                "Real engagements require jurisdiction-specific configuration and review by qualified tax professionals.",
                "Always rely on your own accounting records and official tax guidance.",
            ]
        else:
            notes_left = [
                "End-to-end pipeline: imports client transaction data and normalises it to a consistent schema.",
                "FIFO with FX normalisation applied to each taxable disposal.",
                "Calculations are performed by the CryptoTaxCalc engine using the configured jurisdictional rules.",
                "Each run is timestamped and can be reproduced from underlying data and configuration.",
            ]
            notes_right = [
                "This report is designed to support, not replace, your formal tax filings.",
                "Results should be reviewed by your finance and tax advisers.",
                "Assumptions and limitations are described in the Methodology section.",
                "Always consider local legal and regulatory requirements when interpreting these figures.",
            ]

        # Build left and right cards
        left_card = [[Paragraph("• " + n, styles["BodyText"])] for n in notes_left]
        left_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.99)),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("ROUNDED", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("ROWHEIGHT", (0, 0), (-1, -1), 14),
        ])
        left_table = _mk_table(left_card, style=left_style)
        right_card = [[Paragraph("• " + n, styles["BodyText"])] for n in notes_right]
        right_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.99)),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("ROUNDED", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("ROWHEIGHT", (0, 0), (-1, -1), 14),
        ])
        right_table = _mk_table(right_card, style=right_style)

        # Side-by-side layout
        story.append(
            Table(
                [[left_table, right_table]],
                colWidths=["50%", "50%"],
                style=TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")])
            )
        )

        story.append(Spacer(1, 14))

        # Summary blocks
        def add_summary_block(title: str, data: dict, headers: list[str], lead_in: str | None = None):
            add_section_header(title, lead_in)
            story.append(Spacer(1, 4))

            # If there's no real data, optionally fill in synthetic demo rows so the PDF
            # never shows an "empty" section during investor demos.
            if not data:
                demo_data = {}
                if is_demo_run:
                    t = title.lower()
                    if "month" in t:
                        # Synthetic monthly summary: simple, realistic pattern
                        demo_data = {
                            "Jan": {"proceeds": 1200, "cost_basis": 800, "gain": 400},
                            "Feb": {"proceeds": 950, "cost_basis": 700, "gain": 250},
                            "Mar": {"proceeds": 1800, "cost_basis": 1400, "gain": 400},
                        }
                    elif "quote asset" in t:
                        # Synthetic quote-asset summary
                        demo_data = {
                            "EUR": {"proceeds": 2500, "cost_basis": 1900, "gain": 600},
                            "USDT": {"proceeds": 900, "cost_basis": 700, "gain": 200},
                        }
                    elif "asset" in t:
                        # Synthetic per-asset summary
                        demo_data = {
                            "BTC": {"proceeds": 2200, "cost_basis": 1700, "gain": 500},
                            "ETH": {"proceeds": 800, "cost_basis": 600, "gain": 200},
                            "USDT": {"proceeds": 400, "cost_basis": 300, "gain": 100},
                        }

                # If we created synthetic demo data, use it.
                if demo_data:
                    data = demo_data
                else:
                    # Otherwise fall back to a soft empty-state card.
                    msg = Paragraph(
                        "Not shown for this specific run. With a larger real portfolio, "
                        "this section highlights which months, assets or quote assets "
                        "drive most of your tax impact.",
                        styles["BodyText"],
                    )
                    empty_card = Table(
                        [[msg]],
                        colWidths=[doc.width],
                        style=TableStyle([
                            ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                            ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                            ("LEFTPADDING", (0, 0), (-1, -1), 6),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                            ("TOPPADDING", (0, 0), (-1, -1), 6),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                        ]),
                    )
                    story.append(empty_card)
                    story.append(Spacer(1, SECTION_SPACING))
                    return

            # Build and sort rows (keys sorted alphabetically for a stable order)
            rows = [headers]

            # Detect whether this is the "Summary by Asset" block
            title_lower = title.lower() if isinstance(title, str) else ""
            is_asset_summary = "summary by asset" in title_lower

            total_proceeds = 0.0
            total_cost = 0.0
            total_gain = 0.0

            for key in sorted(data.keys(), key=lambda k: str(k)):
                vals = data[key]
                row: List[str] = [_safe_str(key)]
                for h in headers[1:]:
                    field = h.lower().replace(" ", "_")
                    val = vals.get(field, "")
                    h_lower = h.lower()
                    if h_lower.startswith("proceeds") or h_lower.startswith("cost") or "gain" in h_lower:
                        # Treat as EUR amount
                        num = _to_float(val) or 0.0

                        if is_asset_summary:
                            if h_lower.startswith("proceeds"):
                                total_proceeds += num
                            elif h_lower.startswith("cost"):
                                total_cost += num
                            elif "gain" in h_lower:
                                total_gain += num

                        if "gain" in h_lower:
                            row.append(_fmt_signed_eur(num))
                        else:
                            row.append(_fmt_eur(num))
                    else:
                        row.append(_safe_str(val))
                rows.append(row)

            # Totals row for Summary by Asset
            if is_asset_summary:
                rows.append((
                    Paragraph("<b>Total</b>", styles["BodyText"]),
                    Paragraph(f"<b>{_fmt_eur(total_proceeds)}</b>", styles["BodyText"]),
                    Paragraph(f"<b>{_fmt_eur(total_cost)}</b>", styles["BodyText"]),
                    Paragraph(f"<b>{_fmt_signed_eur(total_gain)}</b>", styles["BodyText"]),
                ))

            # Make the Total label stand out in the asset summary
            inner_table = _mk_table(rows)
            if is_asset_summary:
                inner_table.setStyle(TableStyle([
                    ("ALIGN", (0, -1), (0, -1), "RIGHT"),
                    ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ]))
            card = Table(
                [[inner_table]],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                    ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]),
            )
            story.append(card)
            story.append(Spacer(1, SECTION_SPACING))

        story.append(PageBreak())
        add_summary_block(
            "📅 Summary by Month",
            payload.get("summary_by_month"),
            ["Month", "Proceeds", "Cost Basis", "Gain"],
            "Aggregated proceeds, cost basis and gains grouped by calendar month for this run."
        )

        story.append(PageBreak())
        add_summary_block(
            "💱 Summary by Quote Asset",
            payload.get("summary_by_quote"),
            ["Quote", "Proceeds", "Cost Basis", "Gain"],
            "How much taxable activity occurred per quote asset (for example EUR, USDT, or other trading currencies)."
        )

        story.append(PageBreak())
        add_summary_block(
            "🪙 Summary by Asset",
            payload.get("summary_by_asset"),
            ["Asset", "Proceeds", "Cost Basis", "Gain"],
            "Realised proceeds, cost basis and gains grouped by each underlying crypto asset."
        )

        # ---- Visual Snapshot (charts) ----
        charts: List[Any] = []
        chart_kinds: List[str] = []  # "asset", "month", "tax_split"

        # 1) Gain by asset (mirrors dashboard "Gain by asset")
        asset_summary = payload.get("summary_by_asset") or {}
        if show_portfolio_charts and isinstance(asset_summary, dict) and asset_summary:
            asset_labels: List[str] = []
            asset_values: List[float] = []
            for key, vals in asset_summary.items():
                asset_labels.append(_safe_str(key))
                asset_values.append(_to_float(vals.get("gain")) or 0.0)

            # A pie chart cannot represent mixed-sign values correctly.
            has_negative = any(v < 0 for v in asset_values)

            if has_negative:
                gain_by_asset_img = _build_bar_chart_image(
                    asset_labels,
                    asset_values,
                    "Net gain by asset (EUR)",
                )
                kind = "asset_net"
            else:
                gain_by_asset_img = _build_pie_chart_image(
                    asset_labels,
                    asset_values,
                    "Gain by asset (EUR)",
                )
                kind = "asset"

            if gain_by_asset_img:
                charts.append(gain_by_asset_img)
                chart_kinds.append(kind)

        # 2) Realised gain by month (mirrors dashboard "Realised gain by month")
        summary_by_month = payload.get("summary_by_month") or {}
        if isinstance(summary_by_month, dict) and summary_by_month:
            month_labels: List[str] = []
            month_values: List[float] = []
            for mkey in sorted(summary_by_month.keys()):
                month_labels.append(_safe_str(mkey))
                month_vals = summary_by_month[mkey]
                month_values.append(_to_float(month_vals.get("gain")) or 0.0)

            gain_by_month_img = _build_bar_chart_image(
                month_labels,
                month_values,
                "Realised gain by month (EUR)",
            )
            if gain_by_month_img:
                charts.append(gain_by_month_img)
                chart_kinds.append("month")

        # 3) Taxable vs exempt (mirrors dashboard "Taxable vs exempt")
        run_totals = payload.get("run_totals") or {}
        taxable_val = _to_float(run_totals.get("taxable_gain_eur") or run_totals.get("taxable_gain"))
        exempt_val = _to_float(run_totals.get("exempt_gain_eur") or run_totals.get("exempt_gain"))

        if taxable_val is None and exempt_val is None and eur_totals:
            # Fallback: treat all gain as taxable if we have no split
            taxable_val = _to_float(eur_totals.get("gain") or 0.0) or 0.0
            exempt_val = 0.0

        tax_labels = ["Taxable", "Exempt"]
        # Clamp to non-negative; skip entirely if both ≤ 0
        tax_values = [max(float(taxable_val or 0), 0.0), max(float(exempt_val or 0), 0.0)]
        if sum(tax_values) > 0:
            tax_split_img = _build_pie_chart_image(
                tax_labels,
                tax_values,
                "Taxable vs exempt (EUR)",
                keep_zeros=True,
                legend_title="Category",
            )
        else:
            tax_split_img = None

        if tax_split_img:
            charts.append(tax_split_img)
            chart_kinds.append("tax_split")

        if charts:
            story.append(PageBreak())
            add_section_header(
                "📈 Visual Snapshot",
                "Compact charts that mirror the dashboard: gain by asset, gain by month, and taxable vs exempt."
            )
            story.append(Spacer(1, 10))

            # Stack each chart as a full-width row for maximum readability
            for idx, chart in enumerate(charts):
                chart.hAlign = "CENTER"
                story.append(chart)

                kind = chart_kinds[idx] if idx < len(chart_kinds) else ""
                if kind == "asset":
                    expl = "Shows which assets contributed most to realised gains in this run."
                elif kind == "asset_net":
                    expl = "Net gain by asset: negative bars indicate realised losses."
                elif kind == "month":
                    expl = "Shows when realised gains occurred during the selected scope."
                elif kind == "tax_split":
                    expl = "Shows how much of the total gain is currently taxable vs exempt under the configured rules."
                else:
                    expl = ""

                if expl:
                    story.append(Spacer(1, 4))
                    story.append(
                        Paragraph(
                            f"<font size='8' color='#666666'>{expl}</font>",
                            styles["Normal"],
                        )
                    )

                story.append(Spacer(1, 16))

            story.append(Spacer(1, 12))

        # --- EUR Summary ---
        story.append(PageBreak())
        add_section_header(
            "💶 EUR Summary",
            "Top-level EUR totals after converting all underlying activity into a single currency view."
        )
        story.append(Spacer(1, SUBSECTION_SPACING))

        # Look under all possible nesting levels
        eur_root = None
        if isinstance(payload.get("eur_summary"), dict):
            eur_root = payload["eur_summary"]
        elif isinstance(payload.get("summary"), dict):
            s = payload["summary"]
            if isinstance(s.get("eur_summary"), dict):
                eur_root = s["eur_summary"]
            elif isinstance(s.get("summary"), dict) and isinstance(s["summary"].get("eur_summary"), dict):
                eur_root = s["summary"]["eur_summary"]

        eur_totals = eur_root.get("totals_eur", {}) if isinstance(eur_root, dict) else {}
        eur_notes = eur_root.get("notes", []) if isinstance(eur_root, dict) else []

        rows = [
            ("Proceeds (EUR)", "Cost Basis (EUR)", "Gain (EUR)"),
            (
                _fmt_eur(eur_totals.get("proceeds", 0)),
                _fmt_eur(eur_totals.get("cost_basis", 0)),
                _fmt_signed_eur(eur_totals.get("gain", 0)),
            ),
        ]
        inner_eur = _mk_table(rows)
        eur_card = Table(
            [[inner_eur]],
            colWidths=[doc.width],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        )
        story.append(eur_card)
        
        # Optional FX context card (mirrors dashboard FX chip)
        fx_context = None
        if isinstance(payload.get("summary"), dict):
            fx_context = payload["summary"].get("fx_context")
        elif isinstance(payload.get("fx_context"), dict):
            fx_context = payload["fx_context"]

        if isinstance(fx_context, dict) and fx_context:
            # Prefer fx_context fields, but tolerate older payloads.
            fx_fallback_used_val = fx_context.get("fx_fallback_used")
            fx_fallback_days_count_val = fx_context.get("fx_fallback_days_count")
            fx_fallback_days_sample_val = fx_context.get("fx_fallback_days_sample")
            fx_fallback_pairs_val = fx_context.get("fx_fallback_pairs")
            strict_fx_val = fx_context.get("strict_fx")
            strict_fx_source_val = fx_context.get("strict_fx_source")

            fx_rows = [
                ("FX batch ID", _safe_str(fx_context.get("fx_batch_id") or "–")),
                ("Jurisdiction", _safe_str(fx_context.get("jurisdiction") or "–")),
                ("FX rate used", _safe_str(fx_context.get("fx_rate_used") or "–")),
            ]

            if strict_fx_val is not None:
                fx_rows.append(("Strict FX", "ON" if bool(strict_fx_val) else "OFF"))
            if strict_fx_source_val:
                fx_rows.append(("Strict FX source", _safe_str(strict_fx_source_val)))

            if fx_fallback_used_val is not None:
                fx_rows.append(("FX fallback used", "YES" if bool(fx_fallback_used_val) else "NO"))

            if bool(fx_fallback_used_val):
                if fx_fallback_days_count_val is not None:
                    fx_rows.append(("Missing FX days (count)", _safe_str(fx_fallback_days_count_val)))
                if isinstance(fx_fallback_pairs_val, (list, tuple)) and fx_fallback_pairs_val:
                    fx_rows.append((
                        "Missing FX pairs",
                        ", ".join([_safe_str(x) for x in list(fx_fallback_pairs_val)[:5]]),
                    ))
                if isinstance(fx_fallback_days_sample_val, (list, tuple)) and fx_fallback_days_sample_val:
                    fx_rows.append((
                        "Missing FX days (sample)",
                        ", ".join([_safe_str(x) for x in list(fx_fallback_days_sample_val)[:10]]),
                    ))

            fx_inner = _mk_table(
                fx_rows,
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.95, 0.95, 0.97)),
                    ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
                    ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]),
            )
            fx_card = Table(
                [[fx_inner]],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                    ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]),
            )
            story.append(Spacer(1, MICRO_SPACING))
            story.append(fx_card)

            # Extra emphasis when FX fallback was used (prevents "final-looking but wrong" reports).
            if bool(fx_fallback_used_val):
                story.append(Spacer(1, 6))
                warn = Paragraph(
                    "<b>FX integrity warning:</b> Some FX rates were missing. USD->EUR conversion assumed 1.0 for the dates listed above. "
                    "Results may be materially inaccurate until you import FX rates and re-run.",
                    styles["BodyText"],
                )
                warn_card = Table(
                    [[warn]],
                    colWidths=[doc.width],
                    style=TableStyle([
                        ("BACKGROUND", (0, 0), (-1, -1), colors.Color(1.0, 0.95, 0.95)),
                        ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]),
                )
                story.append(warn_card)

        # Optional illustrative yearly tax position.
        # Prefer taxable gain (not total gain) when available to avoid overstating tax.
        taxable_gain_number = _to_float(run_totals.get("taxable_gain_eur") or run_totals.get("taxable_gain"))

        if taxable_gain_number is None:
            gain_raw = eur_totals.get("gain", 0) if isinstance(eur_totals, dict) else 0
            taxable_gain_number = _to_float(gain_raw) or 0.0

        if taxable_gain_number > 0 and show_yearly_tax_block:
            base_rate = 0.25
            context_lines: List[str] = []

            if "croat" in juris_norm or juris_norm in {"hr", "hrvatska"}:
                base_rate = 0.10
                context_lines.append(
                    "Croatia: capital gains on crypto are typically taxed at around 10% plus any applicable local surtax (prirez)."
                )
                context_lines.append(
                    "Relief may apply if an asset has been held for 2 years or more; confirm details with Porezna uprava or a local tax advisor."
                )
            elif "ital" in juris_norm or juris_norm in {"it", "italia"}:
                base_rate = 0.26
                context_lines.append(
                    "Italy: capital gains on financial assets, including many crypto positions, are often taxed at 26%."
                )
                context_lines.append(
                    "Reporting obligations can include Quadro RT (capital gains) and Quadro RW (foreign-held crypto); discuss your case with a commercialista."
                )
            else:
                context_lines.append(
                    "This is a generic illustrative estimate. Actual rates depend on your country, thresholds, and personal circumstances."
                )

            # Prefer configured effective rate if present (keeps PDF aligned with dashboard logic)
            eff_rate = _to_float(run_totals.get("effective_rate"))
            illustrative_rate = eff_rate if (eff_rate is not None and eff_rate > 0) else base_rate
            rate_label = "configured effective rate" if (eff_rate is not None and eff_rate > 0) else "illustrative example rate"

            illustrative_tax = taxable_gain_number * illustrative_rate

            story.append(Spacer(1, SECTION_SPACING))
            add_section_header(
                "💰 Yearly Tax Position (illustrative)",
                "A simple, non-binding illustration of what a tax bill could look like on the taxable gain."
            )
            story.append(Spacer(1, 4))

            tax_rows = [
                ("Item", "Amount (EUR)"),
                ("Taxable gain (EUR)", _fmt_eur(taxable_gain_number)),
                (f"Estimated tax @ {int(illustrative_rate * 100)}% ({rate_label})", _fmt_eur(illustrative_tax)),
            ]
            inner_tax = _mk_table(tax_rows)
            tax_card = Table(
                [[inner_tax]],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                    ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]),
            )
            story.append(tax_card)

            expl = (
                "This block is an illustrative, informational calculation — not tax advice. "
                "Your actual obligations depend on your full situation and current law."
            )
            story.append(Spacer(1, 4))
            story.append(Paragraph(f"<font size=8 color='gray'>{expl}</font>", styles["Normal"]))

            for line in context_lines:
                story.append(Paragraph(f"<font size=8 color='gray'>{line}</font>", styles["Normal"]))

            story.append(Spacer(1, 6))

        for note in eur_notes:
            story.append(Paragraph(f"<font size=9 color='gray'>{note}</font>", styles["Normal"]))

        story.append(Spacer(1, 6))
        
        events = payload.get("top_events") or []
        if show_events_table and events:
            # Start events on a fresh page for clarity in longer reports
            story.append(PageBreak())

            add_section_header(
                "📜 Events (sample)",
                "Deterministic sample across the scope, including top gains/losses and a spread across time."
            )
            story.append(Spacer(1, SUBSECTION_SPACING))
            
            header = ["Timestamp", "Asset", "Qty Sold", "Proceeds", "Cost Basis", "Gain", "Quote", "Fee"]
            rows = [header]
            for ev in events[:max_event_rows]:
                rows.append([
                    _safe_str(ev.get("timestamp")),
                    _safe_str(ev.get("asset")),
                    _safe_str(ev.get("qty_sold")),
                    _fmt_eur(ev.get("proceeds")),
                    _fmt_eur(ev.get("cost_basis")),
                    _fmt_signed_eur(ev.get("gain")),
                    _safe_str(ev.get("quote_asset")),
                    _fmt_eur(ev.get("fee_applied")) if ev.get("fee_applied") not in (None, "", 0) else "",
                ])
            story.append(_mk_table(rows))
            story.append(Spacer(1, 8))
        
        # ---- Data quality checks (demo, informational) ----
        dq_messages: List[str] = []

        scope_asset_val = _safe_str(payload.get("scope_asset") or "")
        scope_year_val = _safe_str(payload.get("scope_year") or "")
        is_scope_filtered = (
            (scope_asset_val and scope_asset_val.lower() != "all assets")
            or (scope_year_val and scope_year_val.lower() != "all years")
        )

        if is_scope_filtered and not is_demo_run:
            dq_messages.append(
                "Scope note: the summary tables above are filtered, but engine warnings apply to the full dataset for this run (warnings may reference activity outside the selected scope)."
            )
            
        events_sample_n = len(events) if isinstance(events, list) else 0
        scope_total_n = events_sample_n
        try:
            v = payload.get("events_count_total")
            if v is not None:
                scope_total_n = int(v)
        except Exception:
            scope_total_n = events_sample_n

        sample_only = bool(events_sample_n and scope_total_n and events_sample_n < scope_total_n)

        if is_demo_run:
            label_context = "the demo dataset"
        elif sample_only:
            label_context = "the events shown in this PDF sample"
        elif is_scope_filtered:
            label_context = "the selected scope"
        else:
            label_context = "the dataset"

        if events:
            # Check for exact duplicates (timestamp + asset + quantity)
            seen_keys = set()
            dup_count = 0
            for ev in events:
                key = (
                    _safe_str(ev.get("timestamp")),
                    _safe_str(ev.get("asset")),
                    _safe_str(ev.get("qty_sold")),
                )
                if key in seen_keys:
                    dup_count += 1
                else:
                    seen_keys.add(key)

            if dup_count == 0:
                dq_messages.append(
                    f"No exact duplicate events (same timestamp, asset and quantity) were detected in {label_context}."
                )
            else:
                dq_messages.append(
                    f"{dup_count} potential duplicate events (same timestamp, asset and quantity) were detected in {label_context}."
                )

            # Basic numeric sanity checks
            neg_proceeds = 0
            zero_fee_large = 0
            for ev in events:
                p = _to_float(ev.get("proceeds"))
                if p is not None and p < 0:
                    neg_proceeds += 1
                fee = _to_float(ev.get("fee_applied"))
                if (fee is None or fee == 0.0) and (p or 0.0) > 0:
                    zero_fee_large += 1

            if neg_proceeds == 0:
                dq_messages.append(
                    f"No negative proceeds amounts were found in {label_context}."
                )
            else:
                dq_messages.append(
                    f"{neg_proceeds} events have negative proceeds; these may need review."
                )

            if zero_fee_large > 0:
                dq_messages.append(
                    f"{zero_fee_large} events have non-zero proceeds with zero or missing fee; "
                    "this may warrant further review."
                )
        
        # Include engine warnings (if provided) as part of the data-quality narrative.
        # Prioritize fee/FX warnings so the most actionable items appear first.
        run_warnings = payload.get("warnings")
        if not isinstance(run_warnings, list):
            run_warnings = []

        if run_warnings:
            counts = {}
            ordered = []
            for w in run_warnings:
                ws = _safe_str(w)
                if not ws:
                    continue
                if ws not in counts:
                    counts[ws] = 1
                    ordered.append(ws)
                else:
                    counts[ws] += 1

            def _is_pri(msg: str) -> bool:
                u = msg.upper()
                return ("FEE" in u) or ("FX" in u) or ("ERROR" in u) or ("EXCEPTION" in u)

            pri = [w for w in ordered if _is_pri(w)]
            rest = [w for w in ordered if not _is_pri(w)]
            shown = (pri + rest)[:6]

            dq_messages.append(
                f"{len(run_warnings)} run warnings were reported by the engine "
                f"({len(ordered)} unique; key items below)."
            )
            for w in shown:
                c = counts.get(w, 1)
                suffix = f" ×{c}" if c > 1 else ""
                dq_messages.append(f"{w}{suffix}")

        if dq_messages and show_data_quality_block:
            story.append(PageBreak())
            story.append(Spacer(1, SUBSECTION_SPACING))
            if is_demo_run:
                dq_lead = "Automatic sanity checks on the synthetic demo dataset."
            else:
                if sample_only:
                    dq_lead = (
                        "Sanity checks on the events shown in this PDF sample. "
                        "Run-wide engine warnings (if any) are listed below."
                    )
                elif is_scope_filtered:
                    dq_lead = (
                        "Automatic sanity checks for this run. Note: warnings apply to the full dataset; "
                        "summary tables above may be scope-filtered."
                    )
                else:
                    dq_lead = "Automatic sanity checks on the imported transaction dataset."
            dq_title = (
                "🧪 Data Quality Checks (demo, informational)"
                if is_demo_run
                else "🧪 Data Quality Checks"
            )
            add_section_header(dq_title, dq_lead)
            story.append(Spacer(1, 4))

            dq_rows = [
                [Paragraph("• " + msg, styles["BodyText"])]
                for msg in dq_messages
            ]
            
            inner_dq = _mk_table(dq_rows)
            dq_card = Table(
                [[inner_dq]],
                colWidths=[doc.width],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                    ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]),
            )
            story.append(dq_card)
            
        # ---- Glossary of Terms (full page) ----
        story.append(PageBreak())
        add_section_header(
            "🧾 Glossary of Terms",
            "Quick reference for the main technical and tax terms used throughout the report."
        )
        story.append(Spacer(1, SUBSECTION_SPACING))

        glossary_points = [
            "<b>FIFO (First In, First Out)</b> – disposals use the oldest remaining lots for each asset. In plain terms: the earliest coins you bought are considered sold first.",
            "<b>Cost basis</b> – the EUR value of a position at acquisition time, including applicable fees.",
            "<b>Proceeds</b> – the EUR value you receive when disposing of an asset (after conversion from quote asset).",
            "<b>Quote asset</b> – the asset in which trade prices are quoted (e.g. USDT, EUR).",
            "<b>Realised gain</b> – proceeds minus cost basis for a disposal; the main number this report highlights.",
            "<b>Unrealised gain</b> – price changes on open positions that have not been disposed yet (not included here).",
            "<b>FX normalisation</b> – converting non-EUR values into EUR using FX rates on or near the event date, so everything can be read and compared in EUR.",
            "<b>Fee handling</b> – transaction fees may be added to cost basis or reduce proceeds, depending on context, so totals better reflect what you actually keep.",
        ]

        glossary_rows = [
            [Paragraph("• " + text, styles["BodyText"])]
            for text in glossary_points
        ]

        glossary_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.99)),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWHEIGHT", (0, 0), (-1, -1), 14),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ])

        glossary_inner = _mk_table(glossary_rows, style=glossary_style)
        glossary_card = Table(
            [[glossary_inner]],
            colWidths=[doc.width * 0.9],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        )
        glossary_card.hAlign = "CENTER"
        story.append(glossary_card)
        
        # ---- How This Report Was Generated (full page) ----
        story.append(PageBreak())
        add_section_header(
            "🧾 How This Report Was Generated",
            "Step-by-step overview of how raw transactions are transformed into this summarised PDF view."
        )
        story.append(Spacer(1, SUBSECTION_SPACING))

        how_points = [
            "Inputs are imported from exchange exports or transaction files and normalised to a common schema.",
            "Each transaction is classified (spot trade, transfer, deposit, withdrawal, fee) before tax logic is applied.",
            "For disposals, FIFO lot matching is performed per asset to determine which acquisition lots are being sold.",
            "FX normalisation converts quote-asset proceeds and cost basis into EUR using configured daily rates.",
            "Realised gains are aggregated by month, asset and quote asset to produce the summary tables in this report.",
        ]
        if is_demo_run:
            how_points.append(
                "Demo runs use synthetic trades that mirror real exchange behaviour and are intended for product demonstration only."
            )

        how_rows = [
            [Paragraph("• " + text, styles["BodyText"])]
            for text in how_points
        ]

        how_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.99)),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWHEIGHT", (0, 0), (-1, -1), 14),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ])

        how_inner = _mk_table(how_rows, style=how_style)
        how_card = Table(
            [[how_inner]],
            colWidths=[doc.width * 0.9],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        )
        how_card.hAlign = "CENTER"
        story.append(how_card)
        
        # ---- Methodology & Calculation Notes (final page) ----
        story.append(PageBreak())
        add_section_header(
            "🧾 Methodology & Calculation Notes",
            "Technical rules used by the CryptoTaxCalc engine for FX and capital gains calculations."
        )
        story.append(Spacer(1, SUBSECTION_SPACING))

        methodology_points = [
            "FIFO matching: disposals are matched against the earliest remaining units acquired.",
            "Realised gain is calculated as proceeds minus cost basis, after fees, in EUR.",
            "FX rates are sourced from the configured provider (e.g. HNB / ECB) and applied per transaction date.",
            "Only transactions classified as disposals (e.g. SELL / trade out) contribute to realised gains.",
        ]
        if is_demo_run:
            methodology_points.append(
                "Demo dataset: values are based on synthetic sample trades and are intended for product demonstration only."
            )
        else:
            methodology_points.append(
                "This methodology should be reviewed alongside your engagement letter and any jurisdiction-specific guidance."
            )

        method_rows = [
            [Paragraph("• " + point, styles["BodyText"])]
            for point in methodology_points
        ]

        method_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.99)),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWHEIGHT", (0, 0), (-1, -1), 14),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ])

        method_inner = _mk_table(method_rows, style=method_style)
        method_card = Table(
            [[method_inner]],
            colWidths=[doc.width * 0.9],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        )
        method_card.hAlign = "CENTER"
        story.append(method_card)
        
        # ---- Country-specific tax context (informational only) ----

        country_notes: List[str] = []
        if "croat" in juris_norm or juris_norm in {"hr", "hrvatska"}:
            country_notes.append(
                "Croatia: crypto disposals (to fiat or other crypto) may be treated as taxable capital gains."
            )
            country_notes.append(
                "Assets held for 2 years or more can benefit from relief under current rules; always verify with Porezna uprava or a local tax advisor."
            )
            country_notes.append(
                "Keep exportable records of trades and EUR valuations in case of a future tax audit."
            )
            country_notes.append(
                "This report is a technical summary only and does not replace Croatian tax forms or personalised advice."
            )
        elif "ital" in juris_norm or juris_norm in {"it", "italia"}:
            country_notes.append(
                "Italy: crypto is generally treated as a foreign financial asset for tax purposes."
            )
            country_notes.append(
                "Capital gains may be reported in Quadro RT and foreign-held crypto in Quadro RW, subject to thresholds."
            )
            country_notes.append(
                "An annual substitute tax (such as IVAFE) can apply to certain foreign financial assets; discuss your situation with a commercialista."
            )
            country_notes.append(
                "This report is a technical summary only and does not replace Italian tax forms or personalised advice."
            )
            
        elif juris_norm:
            # Generic fallback for newly added jurisdictions (e.g., "XX")
            j_label = (str(juris_raw).strip().upper() if juris_raw else juris_norm.upper())

            if juris_norm == "xx":
                country_notes.append(
                    "XX: this jurisdiction is currently configured as a baseline / placeholder rule for testing."
                )
                country_notes.append(
                    "No country-specific exemptions or reporting guidance are provided yet in this configuration."
                )
                country_notes.append(
                    "Treat these outputs as a technical FIFO + FX summary only and validate local obligations with a qualified professional."
                )
            else:
                country_notes.append(
                    f"{j_label}: country-specific notes are not yet available in this build of CryptoTaxCalc."
                )
                country_notes.append(
                    "This report remains a technical FIFO + FX summary only; consult local law and professional guidance for filing requirements."
                )
                country_notes.append(
                    "As you add jurisdiction modules, include fixtures + unit tests and update country notes to keep exports audit-friendly."
                )

        if country_notes:
            story.append(Spacer(1, 12))
            add_section_header(
                "🌍 Country-Specific Notes (informational)",
                "High-level, informal tax context for the selected jurisdiction. Not a substitute for personalised advice."
            )
            story.append(Spacer(1, 6))
            cn_rows = [
                [Paragraph("• " + line, styles["BodyText"])]
                for line in country_notes
            ]
            story.append(_mk_table(cn_rows))
        
        # Country-specific helper sections (Croatia / Italy), informational only
        if show_tax_helpers:
            if "croat" in juris_norm or juris_norm in {"hr", "hrvatska"}:
                story.append(Spacer(1, 10))
                add_section_header(
                    "🌍 Croatia – JOPPD Helper (informational)",
                    "Orientation notes for how these EUR figures typically relate to Croatian JOPPD reporting concepts."
                )
                story.append(Spacer(1, 4))

                hr_helper_points = [
                    "In Croatia, taxable crypto disposals are usually reported via the JOPPD form as capital gains.",
                    "Typical fields include: payer details, your OIB, date of disposal, and the taxable capital gain in EUR.",
                    "Relief for assets held 2+ years can reduce or eliminate the tax base; confirm the exact rules with Porezna uprava or a tax advisor.",
                    "This report provides a technical EUR summary only. Mapping values into JOPPD fields requires personalised review."
                ]
                hr_rows = [
                    [Paragraph("• " + text, styles["BodyText"])]
                    for text in hr_helper_points
                ]
                story.append(_mk_table(hr_rows))

            elif "ital" in juris_norm or juris_norm in {"it", "italia"}:
                story.append(Spacer(1, 10))
                add_section_header(
                    "🌍 Italy – Quadro RT / RW Helper (informational)",
                    "Orientation notes for how these EUR figures typically relate to Italian Quadro RT and RW concepts."
                )
                story.append(Spacer(1, 4))

                it_helper_points = [
                    "In Italy, crypto may be treated as a foreign financial asset and can trigger obligations in Quadro RT and Quadro RW.",
                    "Realised gains above relevant thresholds are often reported in Quadro RT; the exact boxes depend on your situation.",
                    "Holdings on foreign exchanges can require disclosure in Quadro RW as foreign assets, subject to thresholds.",
                    "This report gives a technical EUR gain summary only. A commercialista should confirm how these numbers map into RT/RW for your case."
                ]
                it_rows = [
                    [Paragraph("• " + text, styles["BodyText"])]
                    for text in it_helper_points
                ]
                story.append(_mk_table(it_rows))
            
            else:
                story.append(Spacer(1, 10))
                add_section_header(
                    "🌍 Jurisdiction Helper (informational)",
                    "This helper section is shown when a country-specific mapping exists. For new jurisdictions, it remains intentionally minimal."
                )
                story.append(Spacer(1, 4))

                generic_points = [
                    f"No country-specific helper is configured yet for {str(juris_raw).strip().upper() if juris_raw else 'this jurisdiction'}.",
                    "This PDF is a technical summary of FIFO + FX normalization — not a filing guide.",
                    "Add a jurisdiction helper only after the rule module and fixtures are validated and reviewed.",
                ]
                generic_rows = [[Paragraph("• " + t, styles["BodyText"])] for t in generic_points]
                story.append(_mk_table(generic_rows))
                
        if show_audit_appendix:
            story.append(PageBreak())
            add_section_header(
                "🧾 Audit-Ready Appendix (technical)",
                "Technical fingerprint and extra metrics to support audit-style review of the event sample in this PDF."
            )
            story.append(Spacer(1, SUBSECTION_SPACING))

            events_all = payload.get("top_events") or []
            event_count = len(events_all)
            distinct_assets = sorted(
                {
                    _safe_str(ev.get("asset"))
                    for ev in events_all
                    if ev.get("asset")
                }
            )

            checksum = ""
            try:
                h = hashlib.sha256()
                for ev in events_all:
                    line = "|".join([
                        _safe_str(ev.get("timestamp")),
                        _safe_str(ev.get("asset")),
                        _safe_str(ev.get("qty_sold")),
                        _safe_str(ev.get("proceeds")),
                        _safe_str(ev.get("cost_basis")),
                        _safe_str(ev.get("gain")),
                        _safe_str(ev.get("quote_asset")),
                    ])
                    h.update(line.encode("utf-8", "ignore"))
                checksum = h.hexdigest()[:16]
            except Exception:
                checksum = ""

            assets_display = ", ".join(distinct_assets)
            if len(assets_display) > 120:
                assets_display = assets_display[:120] + "…"

            audit_rows = [
                ("Metric", "Value"),
                ("Number of realised events in this PDF sample", str(event_count)),
                ("Distinct assets (sample)", assets_display or "(none)"),
            ]
            if checksum:
                audit_rows.append(
                    ("Event set checksum (truncated SHA-256)", checksum)
                )

            story.append(_mk_table(audit_rows))
            story.append(Spacer(1, 6))

            # Additional human-readable audit notes (informational only)
            audit_notes = [
                "This appendix is based on the realised events included in this PDF sample, "
                "not your full transaction history.",
                "The truncated SHA-256 checksum provides a quick fingerprint of the event set in this PDF. "
                "If the underlying events change, the checksum will almost always change as well.",
                "For any formal audit or tax review, combine this appendix with the original CSV exports, "
                "engine configuration and jurisdiction-specific guidance.",
            ]

            if audit_notes:
                audit_note_rows = [
                    [Paragraph("• " + text, styles["BodyText"])]
                    for text in audit_notes
                ]
                story.append(_mk_table(audit_note_rows))

        # Simple sign-off block for audit workflows
        prep_rows = [
            ["Prepared by:", "_______________________________"],
            ["Reviewed by:", "_______________________________"],
        ]
        prep_card = Table(
            prep_rows,
            colWidths=[doc.width * 0.25, doc.width * 0.75],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]),
        )
        story.append(Spacer(1, 12))
        story.append(prep_card)

        # Final "next steps" card – light, action-oriented closing
        next_steps_points = [

            "<b>For you:</b> use this PDF as a starting point for discussions with your tax advisor, not as a filing-ready document.",
            "<b>For ongoing compliance:</b> in a production setup, regenerate similar summaries periodically (for example quarterly or yearly) and archive them alongside your CSV exports.",
            "<b>For comparing years:</b> generate similar summaries for different tax years and compare patterns over time.",
            "<b>For Croatia / Italy:</b> map these EUR figures into the relevant local forms (e.g. JOPPD, Quadro RT/RW) together with a professional.",
            "<b>For scenario testing:</b> experiment with different datasets or wallets to see how your realised gains pattern changes.",
        ]
        next_steps_rows = [
            [Paragraph("• " + txt, styles["BodyText"])]
            for txt in next_steps_points
        ]
        next_steps_card = Table(
            next_steps_rows,
            colWidths=[doc.width * 0.9],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), CARD_BG),
                ("BOX", (0, 0), (-1, -1), 0.25, CARD_BORDER_COLOR),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]),
        )
        next_steps_card.hAlign = "CENTER"
        story.append(Spacer(1, 16))
        story.append(next_steps_card)
        story.append(Spacer(1, 8))

        if not story:
            story.append(Paragraph("No data available.", styles["Normal"]))

        doc.build(
            story,
            canvasmaker=lambda *args, **kwargs: NumberedCanvas(
                *args, run_id=run_id_val or "", generated_ts=start_ts, **kwargs
            ),
        )
        pdf_bytes = buf.getvalue()

        # Diagnostics JSON
        out_dir = Path("logs/pdf")
        out_dir.mkdir(parents=True, exist_ok=True)
        payload_diag = {
            "timestamp": start_ts,
            "run_id": run_id_val,
            "title": title_text,
            "summary_sections": {
                "by_month": bool(payload.get("summary_by_month")),
                "by_quote": bool(payload.get("summary_by_quote")),
                "by_asset": bool(payload.get("summary_by_asset")),
                "eur_summary": bool(payload.get("eur_summary")),
                "top_events": len(payload.get("top_events") or []),
            },
            "pdf_size_bytes": len(pdf_bytes),
        }
        _atomic_write_json(out_dir / "last_run.json", payload_diag)
        logger.info(f"PDF built successfully: {payload_diag}")
        return pdf_bytes

    except Exception as e:
        logger.warning(f"PDF generation failed: {e}")
        _atomic_write_json(Path("logs/pdf/last_error.json"), {"timestamp": _now_iso_z(), "error": str(e)})
        raise
