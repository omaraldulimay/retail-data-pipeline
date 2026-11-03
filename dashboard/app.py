"""Dashboard for the retail data pipeline. Start with `streamlit run dashboard/app.py`."""

import html
import json
import os
from datetime import date
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from pipeline.warehouse import WAREHOUSE_PATH

# Categorical slots in a fixed order (validated for colour-vision deficiencies).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
STATUS = {"ok": "#0ca30c", "knapp": "#fab219", "kritisch": "#ec835a", "ausverkauft": "#d03b3b"}
STATUS_ICON = {"ok": "●", "knapp": "▲", "kritisch": "◆", "ausverkauft": "■"}
# Status of a "this week" item; the icon carries the colour, the label carries the meaning.
STATUS_LABEL = {"ok": "In Ordnung", "knapp": "Beobachten", "kritisch": "Handeln"}
PRIORITY_STATUS = {"jetzt bestellen": "kritisch", "bald bestellen": "knapp"}
CHANNEL_LABELS = {"webshop": "Webshop", "app": "App", "marketplace": "Marktplatz"}
SOURCE_LABELS = {"shop": "Shop", "logistics": "Logistik", "warehouse": "Lager"}
CATEGORIES = ["Kerzen", "Keramik", "Textilien", "Deko", "Küche"]
MAX_REJECTED_SHARE = 0.02  # same limits as the Dagster checks
MAX_SOURCE_LAG_DAYS = 1
FRESHNESS_HOURS = 26
DBT_RESULTS = Path(__file__).resolve().parent.parent / "dbt_project" / "target" / "run_results.json"
REPO_URL = "https://github.com/omaraldulimay/retail-data-pipeline"

INK = "#2B2724"
MUTED = "#6B635B"
HAIRLINE = "#E3DCD0"
SERIF = '"Fraunces", "Cormorant Garamond", Georgia, "Times New Roman", serif'
SANS = '"Inter", -apple-system, "Helvetica Neue", Arial, sans-serif'

st.set_page_config(page_title="Datenbericht · Nordhafen Living", page_icon="🕯️", layout="wide")

# The theme in .streamlit/config.toml sets colours and fonts; this only adds the editorial details.
st.markdown(
    f"""
    <style>
    #MainMenu, footer, [data-testid="stDecoration"] {{ visibility: hidden; }}
    header[data-testid="stHeader"] {{ background: transparent; }}
    .block-container {{ max-width: 1180px; padding-top: 3.5rem; padding-bottom: 5rem; }}
    h1 {{ letter-spacing: -0.01em; line-height: 1.1; padding: 0 0 .4rem; }}
    h2, h3 {{ letter-spacing: -0.005em; }}
    .kicker {{ font-size: .72rem; letter-spacing: .18em; text-transform: uppercase; color: {MUTED};
              margin-bottom: .6rem; }}
    .subline {{ color: {MUTED}; font-size: .92rem; margin-bottom: 1.2rem; }}
    .rule {{ border: 0; border-top: 1px solid {HAIRLINE}; margin: 2rem 0 1.6rem; }}
    .section {{ font-family: {SERIF}; font-size: 1.7rem; margin: .2rem 0 .3rem; }}
    .section-note {{ color: {MUTED}; font-size: .9rem; margin-bottom: 1.1rem; }}
    .hero {{ padding: .2rem 0; }}
    .hero .label {{ font-size: .72rem; letter-spacing: .14em; text-transform: uppercase;
                   color: {MUTED}; }}
    .hero .number {{ font-family: {SERIF}; font-size: clamp(1.8rem, 2.5vw, 2.6rem);
                    font-weight: 400; line-height: 1.15;
                    margin: .25rem 0 .1rem; white-space: nowrap; }}
    .hero .note {{ color: {MUTED}; font-size: .85rem; min-height: 1.3rem; }}
    .card {{ background: #FFFFFF; border: 1px solid {HAIRLINE}; border-radius: 14px;
            padding: 1.3rem 1.4rem; height: 100%; }}
    .card.quiet {{ background: #EFEAE2; }}
    .card .status {{ font-size: .75rem; letter-spacing: .08em; text-transform: uppercase;
                    color: {MUTED}; }}
    .card .status .icon {{ font-size: .95rem; margin-right: .35rem; }}
    .card .headline {{ font-family: {SERIF}; font-size: 1.35rem; line-height: 1.25;
                      margin: .5rem 0 .8rem; }}
    .card .lines {{ list-style: none; margin: 0; padding: 0; font-size: .88rem; }}
    .card .lines li {{ display: flex; flex-direction: column; gap: .1rem;
                      padding: .4rem 0; border-top: 1px solid {HAIRLINE}; }}
    .card .lines li span:last-child {{ color: {MUTED}; font-size: .82rem; }}
    .card .text {{ font-size: .88rem; color: {MUTED}; line-height: 1.5; }}
    .tile .name {{ font-family: {SERIF}; font-size: 1.2rem; }}
    .tile .share {{ font-family: {SERIF}; font-size: 2.1rem; line-height: 1.2; margin-top: .3rem; }}
    .tile .bar {{ height: 3px; background: {HAIRLINE}; border-radius: 2px; margin: .5rem 0 .7rem; }}
    .tile .bar div {{ height: 3px; background: #2F4A3F; border-radius: 2px; }}
    .tile .meta {{ font-size: .8rem; color: {MUTED}; }}
    section[data-testid="stSidebar"] .block-container {{ padding-top: 2rem; }}
    .side-title {{ font-family: {SERIF}; font-size: 1.3rem; margin: .2rem 0 .6rem; }}
    section[data-testid="stSidebar"] .rule {{ margin: 1.4rem 0 1.2rem; }}
    .status-list {{ list-style: none; margin: 0; padding: 0; font-size: .84rem; }}
    .status-list li {{ padding: .45rem 0; border-top: 1px solid {HAIRLINE}; }}
    .status-list .value {{ display: block; color: {MUTED}; font-size: .78rem;
                          margin-left: 1.3rem; }}
    @media (max-width: 1100px) {{
      [data-testid="stHorizontalBlock"] {{ flex-wrap: wrap; row-gap: 1rem; }}
      [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {{
        min-width: 220px; flex: 1 1 220px; }}
    }}
    .legend-note {{ color: {MUTED}; font-size: .82rem; margin: -.3rem 0 .8rem; }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 1.8rem; border-bottom: 1px solid {HAIRLINE}; }}
    .stTabs [data-baseweb="tab"] {{ padding: .6rem 0; }}
    [data-testid="stExpander"] {{ border: 1px solid {HAIRLINE}; border-radius: 12px;
                                 background: transparent; }}
    </style>
    """,
    unsafe_allow_html=True,
)


@alt.theme.register("nordhafen", enable=True)
def nordhafen_theme() -> alt.theme.ThemeConfig:
    """Quiet charts: no frame, hairline grid, muted labels, 2px lines."""
    axis = {
        "grid": True,
        "gridColor": HAIRLINE,
        "gridWidth": 0.5,
        "domain": False,
        "ticks": False,
        "labelColor": MUTED,
        "labelFont": "Inter",
        "labelFontSize": 11,
        "labelPadding": 8,
        "titleColor": MUTED,
        "titleFont": "Inter",
        "titleFontSize": 11,
        "titleFontWeight": "normal",
        "titlePadding": 12,
    }
    return {
        "config": {
            "background": "transparent",
            "view": {"stroke": None},
            "axis": axis,
            "legend": {
                "labelColor": INK,
                "labelFont": "Inter",
                "labelFontSize": 12,
                "symbolType": "circle",
            },
            "line": {"strokeWidth": 2},
            "range": {"category": SERIES},
        }
    }


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    try:
        con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    except duckdb.IOException:
        # DuckDB has one writer at a time; while a pipeline run writes, readers wait.
        st.info("Die Pipeline aktualisiert gerade die Daten. Bitte in ein paar Sekunden neu laden.")
        st.stop()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def euro(value: float) -> str:
    return f"{value:,.0f} €".replace(",", ".")


def euro_compact(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}".replace(".", ",") + " Mio. €"
    return euro(value)


def percent(value: float) -> str:
    return f"{value * 100:.1f} %".replace(".", ",")


def number(value: float) -> str:
    return f"{int(value):,}".replace(",", ".")


# Vega-Lite formats numbers the US way, so axis labels swap the separator explicitly.
GERMAN_THOUSANDS = "replace(format(datum.value, ',.0f'), ',', '.')"


def decimal(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def signed_percent(value: float) -> str:
    return f"{value:+.1f} %".replace(".", ",")


def day_month(value) -> str:
    return f"{pd.Timestamp(value):%d.%m.}"


def full_date(value) -> str:
    return f"{pd.Timestamp(value):%d.%m.%Y}"


def thin_bars(chart: alt.Chart) -> alt.Chart:
    return chart.mark_bar(size=14, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)


def with_table(chart: alt.Chart, table: pd.DataFrame) -> None:
    # Some series colours are weak on the warm background, so every chart has a table view.
    st.altair_chart(chart, theme=None, width="stretch")
    with st.expander("Als Tabelle anzeigen"):
        st.dataframe(table, hide_index=True, width="stretch")


def section(title: str, note: str | None = None) -> None:
    st.markdown("<hr class='rule'>", unsafe_allow_html=True)
    st.markdown(f"<div class='section'>{title}</div>", unsafe_allow_html=True)
    if note:
        st.markdown(f"<div class='section-note'>{note}</div>", unsafe_allow_html=True)


def status_line(status: str) -> str:
    color = STATUS[status]
    return (
        f"<div class='status'><span class='icon' style='color:{color}'>{STATUS_ICON[status]}"
        f"</span>{STATUS_LABEL[status]}</div>"
    )


@st.cache_resource(ttl=900)
def sync_warehouse() -> bool:
    from pipeline.cloud import download_warehouse

    return download_warehouse()


# In Azure the warehouse lives in blob storage; fetch it at most every 15 minutes.
if os.getenv("AZURE_STORAGE_ACCOUNT_URL"):
    sync_warehouse()

if not WAREHOUSE_PATH.exists():
    st.error(
        "Noch keine Daten. Starte zuerst die Pipeline: "
        "`dagster job execute -m orchestration.definitions -j daily_pipeline`"
    )
    st.stop()

latest = query("select max(order_date) as d from marts.agg_daily_sales")["d"][0]
last_load = pd.Timestamp(query("select max(finished_at) as t from meta.load_log")["t"][0])
hours_old = (pd.Timestamp.now() - last_load).total_seconds() / 3600
missing = query("select * from quality.dq_missing_files order by missing_date")


def newest_per_source() -> dict[str, date | None]:
    """Newest business date per source, with the same logic as the Dagster check sources_fresh."""
    files = query("select file_name from meta.loaded_files")["file_name"]
    return {
        "Shop-API": query(
            "select max(cast(payload ->> 'created_at' as timestamp)::date) as d "
            "from raw.shop_orders"
        )["d"][0],
        "Warenwirtschaft": query(
            "select max(cast(snapshot_date as date)) as d from raw.wms_stock_snapshots"
        )["d"][0],
        "Logistikpartner": max(
            (date.fromisoformat(f.removeprefix("versand_").removesuffix(".csv")) for f in files),
            default=None,
        ),
    }


def dbt_test_summary() -> tuple[int, int] | None:
    """(passed, total) of the last dbt run, if its result file is available."""
    try:
        results = json.loads(DBT_RESULTS.read_text())["results"]
    except (OSError, ValueError, KeyError):
        return None
    tests = [r["status"] for r in results if r["unique_id"].startswith("test.")]
    return (tests.count("pass"), len(tests)) if tests else None


def status_item(status: str, label: str, value: str) -> str:
    return (
        f"<li><span style='color:{STATUS[status]}'>{STATUS_ICON[status]}</span> {label}"
        f"<span class='value'>{value}</span></li>"
    )


def pipeline_status() -> str:
    fresh_load = hours_old <= FRESHNESS_HOURS
    items = [
        status_item(
            "ok" if fresh_load else "knapp",
            "Ladelauf aktuell" if fresh_load else "Ladelauf veraltet",
            f"zuletzt {last_load:%d.%m.%Y, %H:%M} Uhr",
        )
    ]
    newest = {n: pd.Timestamp(d).date() for n, d in newest_per_source().items() if d is not None}
    reference = max(newest.values())
    for name, day in newest.items():
        lagging = (reference - day).days > MAX_SOURCE_LAG_DAYS
        items.append(
            status_item(
                "knapp" if lagging else "ok",
                f"{name} {'verzögert' if lagging else 'aktuell'}",
                f"Daten bis {full_date(day)}",
            )
        )
    tests = dbt_test_summary()
    if tests:
        passed, total = tests
        items.append(
            status_item(
                "ok" if passed == total else "kritisch",
                f"{passed} von {total} dbt-Tests bestanden",
                "letzter Lauf der Modelle",
            )
        )
    n_rejected = int(query("select count(*) as n from quality.dq_rejected_rows")["n"][0])
    items.append(
        status_item("ok", f"{number(n_rejected)} Zeilen in Quarantäne", "mit Grund erfasst")
    )
    items.append(
        status_item(
            "knapp", f"{len(missing)} Versanddatei fehlt", full_date(missing["missing_date"][0])
        )
        if len(missing) == 1
        else status_item(
            "knapp" if len(missing) else "ok",
            f"{len(missing)} Versanddateien fehlen" if len(missing) else "Alle Dateien da",
            "Logistikpartner",
        )
    )
    return f"<ul class='status-list'>{''.join(items)}</ul>"


with st.sidebar:
    st.markdown("<div class='side-title'>Filter</div>", unsafe_allow_html=True)
    months = st.select_slider(
        "Zeitraum", options=[1, 3, 6, 12], value=12, format_func=lambda m: f"{m} Monate"
    )
    categories = st.multiselect("Kategorie", CATEGORIES, default=CATEGORIES)
    channels = st.multiselect(
        "Kanal", list(CHANNEL_LABELS), default=list(CHANNEL_LABELS), format_func=CHANNEL_LABELS.get
    )
    st.markdown(
        "<hr class='rule'><div class='side-title'>Pipeline-Status</div>", unsafe_allow_html=True
    )
    st.markdown(pipeline_status(), unsafe_allow_html=True)
    st.markdown(
        "<hr class='rule'><div class='side-title'>So entstehen die Zahlen</div>"
        "<div class='text' style='font-size:.85rem;color:" + MUTED + ";line-height:1.55'>"
        "Jeden Morgen um 6 Uhr lädt Dagster Shop, Warenwirtschaft und Versanddaten. "
        "dbt prüft und modelliert sie in DuckDB; fehlerhafte Zeilen landen mit Begründung "
        "in der Quarantäne. "
        f"<a href='{REPO_URL}' target='_blank'>Quellcode auf GitHub</a></div>",
        unsafe_allow_html=True,
    )

if not categories or not channels:
    st.warning("Bitte mindestens eine Kategorie und einen Kanal auswählen.")
    st.stop()

since = (pd.Timestamp(latest) - pd.DateOffset(months=months)).date()
before = (pd.Timestamp(since) - pd.DateOffset(months=months)).date()
period = f"order_date > date '{since}'"
all_selected = len(categories) == len(CATEGORIES) and len(channels) == len(CHANNEL_LABELS)
cat_sql = ", ".join(f"'{c}'" for c in categories)
chan_sql = ", ".join(f"'{c}'" for c in channels)
freshness = (
    ("ok", "Daten aktuell")
    if hours_old <= FRESHNESS_HOURS
    else ("knapp", f"Letzter Ladelauf vor {int(hours_old // 24)} Tagen")
)


def line_scope(low: date, high: date) -> str:
    """Filter for fct_order_lines: period, categories and channels."""
    return (
        f"order_date > date '{low}' and order_date <= date '{high}' and status <> 'cancelled' "
        f"and category in ({cat_sql}) and channel in ({chan_sql})"
    )


def order_scope(low: date, high: date) -> str:
    """Filter for fct_orders: orders in the period that contain a chosen category."""
    return (
        f"order_date > date '{low}' and order_date <= date '{high}' and status <> 'cancelled' "
        f"and channel in ({chan_sql}) and order_id in "
        f"(select order_id from marts.fct_order_lines where category in ({cat_sql}))"
    )


def full_weeks(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    """Drops the partial first and last week, which would look like a drop."""
    start = pd.to_datetime(frame[column])
    inside = (start >= pd.Timestamp(since) + pd.Timedelta(days=1)) & (
        start + pd.Timedelta(days=6) <= pd.Timestamp(latest)
    )
    return frame[inside]


def week_label(day) -> str:
    return f"KW {pd.Timestamp(day).isocalendar().week} · {full_date(day)}"


active_filters = [
    ", ".join(categories) if len(categories) < len(CATEGORIES) else "",
    ", ".join(CHANNEL_LABELS[c] for c in channels) if len(channels) < len(CHANNEL_LABELS) else "",
]
filter_note = " · Filter: " + " · ".join(f for f in active_filters if f) if not all_selected else ""

st.markdown("<div class='kicker'>Nordhafen Living · Datenbericht</div>", unsafe_allow_html=True)
st.title("Das Jahr in Zahlen")
st.markdown(
    f"<div class='subline'>{full_date(pd.Timestamp(since) + pd.Timedelta(days=1))} bis "
    f"{full_date(latest)} · Stand {last_load:%d.%m.%Y, %H:%M} Uhr · "
    f"<span style='color:{STATUS[freshness[0]]}'>{STATUS_ICON[freshness[0]]}</span> "
    f"{freshness[1]}{html.escape(filter_note)}</div>",
    unsafe_allow_html=True,
)


def kpis(low: date, high: date) -> dict | None:
    """Headline numbers for the days after `low` up to and including `high`."""
    row = query(f"""
        select sum(revenue) as revenue, count(distinct order_id) as orders
        from marts.fct_order_lines where {line_scope(low, high)}
    """).iloc[0]
    if pd.isna(row["revenue"]):
        return None
    orders = query(f"""
        select avg(is_returned::integer) as return_rate,
               avg(is_on_time::integer) filter (where delivered_at is not null) as on_time
        from marts.fct_orders where {order_scope(low, high)}
    """).iloc[0]
    return {**row.to_dict(), **orders.to_dict()}


current = kpis(since, latest)
previous = kpis(before, since)
if current is None:
    st.info("Für diese Auswahl gibt es im Zeitraum keine Bestellungen.")
    st.stop()


def change(key: str, as_points: bool = False) -> str:
    """Change against the previous period of the same length, as text with an arrow."""
    if previous is None or pd.isna(previous[key]):
        return ""
    if as_points:
        diff = (current[key] - previous[key]) * 100
        text = f"{abs(diff):.1f} Pkt.".replace(".", ",")
    else:
        diff = (current[key] / previous[key] - 1) * 100
        text = f"{abs(diff):.1f} %".replace(".", ",")
    arrow = "▲" if diff > 0 else "▼" if diff < 0 else "●"
    return f"{arrow} {text} zur Vorperiode"


days_in_period = max((pd.Timestamp(latest) - pd.Timestamp(since)).days, 1)
heroes = [
    (
        "Umsatz",
        euro_compact(current["revenue"]),
        change("revenue") or f"Ø {euro(current['revenue'] / days_in_period)} pro Tag",
    ),
    (
        "Bestellungen",
        number(current["orders"]),
        change("orders") or f"Ø {number(current['orders'] / days_in_period)} pro Tag",
    ),
    (
        "Retourenquote",
        percent(current["return_rate"]),
        change("return_rate", True) or "Anteil retournierter Bestellungen",
    ),
    (
        "Pünktlich geliefert",
        percent(current["on_time"]),
        change("on_time", True) or "Zustellung binnen 5 Tagen",
    ),
]
for col, (label, value, note) in zip(st.columns(4), heroes, strict=True):
    col.markdown(
        f"<div class='hero'><div class='label'>{label}</div><div class='number'>{value}</div>"
        f"<div class='note'>{note}</div></div>",
        unsafe_allow_html=True,
    )

# --- Diese Woche wichtig -------------------------------------------------------------------
reorder = query("""
    select sku, product_name, category, on_hand, days_of_cover, expected_stockout_date,
           reorder_by_date, reorder_qty, priority
    from marts.rec_reorder
    order by case priority when 'jetzt bestellen' then 0 else 1 end, days_of_cover
""")
reorder = reorder[reorder["category"].isin(categories)]
recent_cutoff = pd.Timestamp(latest) - pd.Timedelta(days=30)
unexpected = query("""
    select order_date, revenue, expected_revenue, deviation_pct, direction
    from marts.alert_sales_anomalies where not is_expected order by order_date desc
""")
recent_unexpected = unexpected[unexpected["order_date"] > recent_cutoff]
rows_total = query("""
    select (select count(*) from staging.stg_order_lines)
         + (select count(*) from raw.wms_stock_snapshots)
         + (select count(*) from raw.logistics_events) as n
""")["n"][0]
all_rejected = query("select count(*) as n from quality.dq_rejected_rows")["n"][0]
reject_share = all_rejected / (rows_total + all_rejected)

section(
    "Diese Woche wichtig", "Was heute Aufmerksamkeit braucht, in der Reihenfolge der Dringlichkeit."
)
now_count = int((reorder["priority"] == "jetzt bestellen").sum())
soon_count = len(reorder) - now_count
reorder_status = "kritisch" if now_count else "knapp" if soon_count else "ok"
top3 = "".join(
    f"<li><span>{html.escape(r.product_name)}</span>"
    f"<span>leer ab {day_month(r.expected_stockout_date)} · "
    f"{number(r.reorder_qty)} Stück</span></li>"
    for r in reorder.head(3).itertuples()
)
if now_count:
    reorder_head = f"{now_count} Artikel jetzt nachbestellen"
elif soon_count:
    reorder_head = f"{soon_count} Artikel bald nachbestellen"
else:
    reorder_head = "Alle Bestände reichen aus"

if len(recent_unexpected):
    last = recent_unexpected.iloc[0]
    anomaly_status = "knapp"
    anomaly_head = f"{len(recent_unexpected)} unerwartete Umsatztage"
    anomaly_text = (
        f"Zuletzt am {full_date(last['order_date'])}: {euro(last['revenue'])}, "
        f"{signed_percent(last['deviation_pct'])} zum üblichen Wert."
    )
else:
    anomaly_status = "ok"
    anomaly_head = "Umsatz im erwarteten Rahmen"
    older = (
        f" Zuletzt auffällig: {full_date(unexpected.iloc[0]['order_date'])} "
        f"({signed_percent(unexpected.iloc[0]['deviation_pct'])})."
        if len(unexpected)
        else ""
    )
    anomaly_text = f"Keine unerwarteten Ausreißer in den letzten 30 Tagen.{older}"

dq_status = "kritisch" if reject_share > MAX_REJECTED_SHARE else "knapp" if len(missing) else "ok"
dq_head = (
    f"{len(missing)} Versanddatei fehlt"
    if len(missing) == 1
    else f"{len(missing)} Versanddateien fehlen"
    if len(missing)
    else "Alle Dateien angekommen"
)
dq_files = (
    f"Fehlt: {', '.join(day_month(d) for d in missing['missing_date'])} · " if len(missing) else ""
)
dq_text = (
    f"{dq_files}{decimal(reject_share * 100)} % der Zeilen liegen in Quarantäne "
    f"(Grenze {MAX_REJECTED_SHARE * 100:.0f} %)."
)

if not all_selected:
    anomaly_text += " (Gesamtumsatz, unabhängig vom Filter.)"
if len(categories) < len(CATEGORIES):
    dq_text += " Qualitätsdaten lassen sich nicht nach Kategorie filtern."

first, second, third = st.columns(3, gap="medium")
first.markdown(
    f"<div class='card'>{status_line(reorder_status)}<div class='headline'>{reorder_head}</div>"
    f"<ul class='lines'>{top3}</ul></div>",
    unsafe_allow_html=True,
)
second.markdown(
    f"<div class='card'>{status_line(anomaly_status)}<div class='headline'>{anomaly_head}</div>"
    f"<div class='text'>{anomaly_text}</div></div>",
    unsafe_allow_html=True,
)
third.markdown(
    f"<div class='card'>{status_line(dq_status)}<div class='headline'>{dq_head}</div>"
    f"<div class='text'>{dq_text}</div></div>",
    unsafe_allow_html=True,
)


def reorder_table(frame: pd.DataFrame) -> None:
    shown = frame.assign(
        Priorität=frame["priority"].map(lambda p: f"{STATUS_ICON[PRIORITY_STATUS[p]]} {p}")
    ).rename(
        columns={
            "product_name": "Artikel",
            "category": "Kategorie",
            "on_hand": "Bestand",
            "days_of_cover": "Reichweite (Tage)",
            "expected_stockout_date": "Voraussichtlich leer am",
            "reorder_by_date": "Bestellen bis",
            "reorder_qty": "Menge",
        }
    )[
        [
            "Artikel",
            "Kategorie",
            "Bestand",
            "Reichweite (Tage)",
            "Voraussichtlich leer am",
            "Bestellen bis",
            "Menge",
            "Priorität",
        ]
    ]
    st.dataframe(
        shown,
        height=36 * (len(shown) + 1) + 4,
        hide_index=True,
        width="stretch",
        column_config={
            "Bestand": st.column_config.NumberColumn(format="localized"),
            "Reichweite (Tage)": st.column_config.NumberColumn(format="localized"),
            "Voraussichtlich leer am": st.column_config.DateColumn(format="DD.MM.YYYY"),
            "Bestellen bis": st.column_config.DateColumn(format="DD.MM.YYYY"),
            "Menge": st.column_config.NumberColumn(format="localized"),
        },
    )


st.write("")
with st.expander(f"Alle {len(reorder)} Nachbestell-Empfehlungen anzeigen"):
    reorder_table(reorder)

# --- Categories -----------------------------------------------------------------------------
section(
    "Sortimente" if len(categories) < len(CATEGORIES) else "Die fünf Sortimente",
    "Umsatzanteil im gewählten Zeitraum und Retourenquote je Kategorie.",
)
shares = query(f"""
    select category, sum(revenue) as revenue from marts.fct_order_lines
    where {line_scope(since, latest)} group by 1
""").set_index("category")["revenue"]
returns = query(f"""
    select c.category as Kategorie,
           count(*) filter (where o.shipped_at is not null) as versendet,
           count(*) filter (where o.is_returned) as retourniert,
           count(*) filter (where o.is_returned)
             / nullif(count(*) filter (where o.shipped_at is not null), 0) as quote,
           mode(o.return_reason) filter (where o.is_returned) as top_reason
    from (
        select distinct order_id, category from marts.fct_order_lines
        where category in ({cat_sql})
    ) as c
    inner join marts.fct_orders as o using (order_id)
    where o.status <> 'cancelled' and o.channel in ({chan_sql}) and o.{period}
    group by c.category order by quote desc
""")
return_rates = returns.set_index("Kategorie")["quote"]
for col, category in zip(st.columns(len(categories), gap="small"), categories, strict=True):
    share = shares.get(category, 0) / shares.sum()
    col.markdown(
        f"<div class='card quiet tile'><div class='name'>{category}</div>"
        f"<div class='share'>{share * 100:.0f} %</div>"
        f"<div class='bar'><div style='width:{share * 100:.0f}%'></div></div>"
        f"<div class='meta'>vom Umsatz · Retouren {percent(return_rates.get(category, 0))}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

st.write("")
st.write("")
sales_tab, delivery_tab, stock_tab, reorder_tab, returns_tab, quality_tab = st.tabs(
    ["Umsatz", "Lieferung", "Lager", "Nachbestellen", "Retouren", "Datenqualität"]
)

with sales_tab:
    daily = query(f"""
        select order_date, sum(revenue) as revenue from marts.fct_order_lines
        where {line_scope(since, latest)} group by 1 order by 1
    """)
    daily["Umsatz"] = daily["revenue"].map(euro)
    events = query(f"""
        select event as Ereignis, greatest(start_date, date '{since}') as start_date,
               least(end_date, date '{latest}') as end_date, expected_effect
        from reference.calendar_events where end_date > date '{since}'
    """)
    events["Wirkung"] = events["expected_effect"].map(
        {"up": "Umsatz steigt", "down": "Umsatz sinkt"}
    )
    flagged = query(f"""
        select order_date, revenue, expected_revenue, deviation_pct, direction, event, is_expected
        from marts.alert_sales_anomalies where {period} order by order_date
    """)
    if not all_selected:
        flagged = flagged.iloc[0:0]  # the anomaly model only looks at total revenue
    flagged["Status"] = flagged["is_expected"].map(
        {True: "durch Ereignis erklärt", False: "unerwartet"}
    )
    flagged["Umsatz"] = flagged["revenue"].map(euro)
    flagged["Üblich"] = flagged["expected_revenue"].map(euro)
    flagged["Abweichung"] = flagged["deviation_pct"].map(signed_percent)
    flagged["Ereignis"] = flagged["event"].fillna("kein bekanntes Ereignis")

    x_axis = alt.X("order_date:T", title=None, axis=alt.Axis(format="%d.%m.", grid=False))
    bands = (
        alt.Chart(events)
        .mark_rect(color=HAIRLINE, opacity=0.55)
        .encode(
            x=alt.X("start_date:T"),
            x2="end_date:T",
            tooltip=[
                "Ereignis:N",
                "Wirkung:N",
                alt.Tooltip("start_date:T", title="Von", format="%d.%m.%Y"),
                alt.Tooltip("end_date:T", title="Bis", format="%d.%m.%Y"),
            ],
        )
    )
    line = (
        alt.Chart(daily)
        .mark_line(color=SERIES[0])
        .encode(
            x=x_axis,
            y=alt.Y(
                "revenue:Q",
                title="Umsatz pro Tag (€)",
                axis=alt.Axis(labelExpr=GERMAN_THOUSANDS),
            ),
            tooltip=[alt.Tooltip("order_date:T", title="Datum", format="%d.%m.%Y"), "Umsatz:N"],
        )
    )
    markers = (
        alt.Chart(flagged)
        .mark_point(filled=True, strokeWidth=1.5, stroke="#F7F4EF")
        .encode(
            x="order_date:T",
            y="revenue:Q",
            size=alt.Size(
                "Status:N",
                scale=alt.Scale(domain=["unerwartet", "durch Ereignis erklärt"], range=[150, 45]),
                legend=None,
            ),
            color=alt.Color(
                "Status:N",
                scale=alt.Scale(
                    domain=["unerwartet", "durch Ereignis erklärt"], range=[INK, "#B8AEA0"]
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("order_date:T", title="Datum", format="%d.%m.%Y"),
                "Status:N",
                "Ereignis:N",
                "Umsatz:N",
                "Üblich:N",
                "Abweichung:N",
            ],
        )
    )
    st.markdown("**Umsatz pro Tag**")
    st.markdown(
        "<div class='legend-note'>● dunkel: unerwartete Abweichung · ● hell: durch ein Ereignis "
        "erklärt · getönte Fläche: Kalenderereignis (Black Friday, Weihnachtsgeschäft, Feiertage, "
        "Winterschlussverkauf, Sommerflaute). Abweichung = mehr als 50 % über oder 30 % unter dem "
        "Median der letzten vier gleichen Wochentage.</div>",
        unsafe_allow_html=True,
    )
    with_table(
        (bands + line + markers).properties(height=340),
        daily.assign(Datum=daily["order_date"].map(full_date))[["Datum", "Umsatz"]],
    )
    if not all_selected:
        st.caption(
            "Auffälligkeiten werden nur für den Gesamtumsatz berechnet und sind bei "
            "aktivem Kategorie- oder Kanalfilter ausgeblendet."
        )

    st.markdown("**Umsatz pro Woche und Kanal**")
    weekly = query(f"""
        select date_trunc('week', order_date)::date as woche, channel, sum(revenue) as umsatz
        from marts.fct_order_lines where {line_scope(since, latest)} group by all order by 1
    """)
    weekly = full_weeks(weekly, "woche")
    weekly["Kanal"] = weekly["channel"].map(CHANNEL_LABELS)
    weekly["Umsatz"] = weekly["umsatz"].map(euro)
    chart = (
        thin_bars(alt.Chart(weekly))
        .encode(
            x=alt.X("woche:T", title=None, axis=alt.Axis(format="%d.%m.", grid=False)),
            y=alt.Y(
                "sum(umsatz):Q",
                title="Umsatz pro Woche (€)",
                axis=alt.Axis(labelExpr=GERMAN_THOUSANDS),
            ),
            color=alt.Color(
                "Kanal:N",
                scale=alt.Scale(domain=list(CHANNEL_LABELS.values()), range=SERIES[:3]),
                legend=alt.Legend(orient="top", title=None),
            ),
            order=alt.Order("Kanal:N"),
            tooltip=[
                alt.Tooltip("woche:T", title="Woche ab", format="%d.%m.%Y"),
                "Kanal:N",
                "Umsatz:N",
            ],
        )
        .properties(height=320)
    )
    weekly["Woche"] = weekly["woche"].map(week_label)
    with_table(chart, weekly[["Woche", "Kanal", "Umsatz"]])
    st.caption("Nur volle Wochen (Mo bis So); angebrochene Wochen am Anfang und Ende fehlen.")

    st.subheader("Bestseller")
    top = query(f"""
        select product_name as Artikel, category as Kategorie, sum(quantity) as Stück,
               round(sum(revenue)) as Umsatz,
               round(sum(margin) / sum(revenue) * 100, 1) as "Marge %"
        from marts.fct_order_lines where {line_scope(since, latest)}
        group by all order by Umsatz desc limit 10
    """)
    top["Marge %"] = top["Marge %"].map(lambda v: f"{decimal(v)} %")
    st.dataframe(
        top,
        hide_index=True,
        width="stretch",
        column_config={
            "Stück": st.column_config.NumberColumn(format="localized"),
            "Umsatz": st.column_config.NumberColumn(format="euro"),
        },
    )

with delivery_tab:
    perf = query(f"""
        select date_trunc('week', shipped_at)::date as woche, carrier as Paketdienst,
               avg(is_on_time::integer) as puenktlich, avg(days_to_deliver) as tage,
               count(delivered_at) as zugestellt
        from marts.fct_orders
        where shipped_at is not null and channel in ({chan_sql}) and order_id in
              (select order_id from marts.fct_order_lines where category in ({cat_sql}))
        group by all having count(delivered_at) > {20 if all_selected else 5} order by 1
    """)
    perf = full_weeks(perf, "woche")
    perf["Pünktlich"] = perf["puenktlich"].map(percent)
    perf["Ø Tage"] = perf["tage"].map(decimal)
    chart = (
        alt.Chart(perf)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=36))
        .encode(
            x=alt.X("woche:T", title=None, axis=alt.Axis(format="%d.%m.", grid=False)),
            y=alt.Y(
                "puenktlich:Q",
                title="Pünktlich zugestellt",
                axis=alt.Axis(format=".0%"),
                scale=alt.Scale(zero=False),
            ),
            color=alt.Color(
                "Paketdienst:N",
                scale=alt.Scale(domain=["DHL", "DPD", "GLS"], range=SERIES[:3]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("woche:T", title="Woche ab", format="%d.%m.%Y"),
                "Paketdienst:N",
                "Pünktlich:N",
                "Ø Tage:N",
                alt.Tooltip("zugestellt:Q", title="Zugestellt"),
            ],
        )
        .properties(height=320)
    )
    # Direct labels at the end of each line (text stays in ink colour, not the series colour).
    ends = (
        alt.Chart(perf[perf["woche"] == perf["woche"].max()])
        .mark_text(align="left", dx=8, color=INK, font="Inter", fontSize=12)
        .encode(x="woche:T", y="puenktlich:Q", text="Paketdienst:N")
    )
    chart = (chart + ends).properties(padding={"right": 40})
    st.caption(
        "Versprechen an Kunden: Zustellung innerhalb von 5 Tagen nach Bestellung. "
        "Im Dezember sind alle Paketdienste überlastet."
    )
    perf["Woche"] = perf["woche"].map(week_label)
    perf["Zugestellt"] = perf["zugestellt"]
    with_table(chart, perf[["Woche", "Paketdienst", "Pünktlich", "Ø Tage", "Zugestellt"]])

with stock_tab:
    stock = query(f"""
        select sku, product_name, category, on_hand, avg_daily_units, days_of_cover, coverage_status
        from marts.agg_stock_coverage
        where category in ({cat_sql})
        order by case coverage_status when 'ausverkauft' then 0 when 'kritisch' then 1
                 when 'knapp' then 2 else 3 end, days_of_cover
    """)
    counts = stock["coverage_status"].value_counts()
    for col, status in zip(st.columns(4), ["ausverkauft", "kritisch", "knapp", "ok"], strict=True):
        col.markdown(
            f"<div class='hero'><div class='label'>"
            f"<span style='color:{STATUS[status]};font-size:1rem'>{STATUS_ICON[status]}</span> "
            f"{status}</div><div class='number'>{counts.get(status, 0)}</div></div>",
            unsafe_allow_html=True,
        )
    stock["Status"] = stock["coverage_status"].map(lambda s: f"{STATUS_ICON[s]} {s}")
    st.dataframe(
        height=36 * (len(stock) + 1) + 4,
        data=stock.rename(
            columns={
                "sku": "SKU",
                "product_name": "Artikel",
                "category": "Kategorie",
                "on_hand": "Bestand",
                "avg_daily_units": "Ø Absatz/Tag",
                "days_of_cover": "Reichweite (Tage)",
            }
        ).drop(columns=["coverage_status"]),
        hide_index=True,
        width="stretch",
        column_config={
            "Bestand": st.column_config.NumberColumn(format="localized"),
            "Ø Absatz/Tag": st.column_config.NumberColumn(format="localized"),
            "Reichweite (Tage)": st.column_config.NumberColumn(format="localized"),
        },
    )
    st.caption(
        "Reichweite = Bestand geteilt durch den durchschnittlichen Absatz der letzten 28 Tage. "
        "Das Lager kennt keine Kanäle, der Kanalfilter gilt hier nicht."
    )

with reorder_tab:
    st.markdown(
        f"{now_count} Artikel sollten jetzt bestellt werden, {soon_count} bald. "
        "Reichweite = Bestand geteilt durch den Absatz der letzten 28 Tage; "
        "Lieferzeit, Sicherheitsbestand und Bestellreichweite sind im dbt-Projekt hinterlegt."
    )
    reorder_table(reorder)
    st.caption(
        "Der Kanalfilter gilt hier nicht, die Empfehlung beruht auf dem Gesamtabsatz je Artikel."
    )
    st.caption(
        "◆ jetzt bestellen: Bestellfrist erreicht oder überschritten · "
        "▲ bald bestellen: noch eine Woche Luft"
    )

with returns_tab:
    returns["Quote"] = returns["quote"].map(percent)
    chart = (
        alt.Chart(returns)
        .mark_bar(size=22, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=SERIES[0])
        .encode(
            y=alt.Y("Kategorie:N", sort="-x", title=None),
            x=alt.X("quote:Q", title="Retourenquote", axis=alt.Axis(format=".0%", tickCount=5)),
            tooltip=[
                "Kategorie:N",
                "Quote:N",
                alt.Tooltip("top_reason:N", title="Häufigster Grund"),
            ],
        )
        .properties(height=alt.Step(34))
    )
    labels = chart.mark_text(align="left", dx=6, color=INK).encode(text="Quote:N")
    with_table(
        chart + labels,
        returns.rename(
            columns={
                "versendet": "Versendet",
                "retourniert": "Retourniert",
                "top_reason": "Häufigster Grund",
            }
        )[["Kategorie", "Quote", "Retourniert", "Versendet", "Häufigster Grund"]],
    )

    reasons = query(f"""
        select return_reason as Grund, count(*) as Retouren
        from marts.fct_orders where is_returned and {order_scope(since, latest)}
        group by 1 order by 2 desc
    """)
    st.dataframe(reasons, hide_index=True, width="stretch")

with quality_tab:
    st.caption(
        "Die Quarantäne wird je Quelle geführt und lässt sich nicht nach Kategorie oder Kanal "
        "filtern; es gilt nur der Zeitraum."
    )
    st.markdown(
        "Fehlerhafte Zeilen werden nicht in die Kennzahlen übernommen, sondern hier mit Grund "
        "gesammelt, damit sie an der Quelle korrigiert werden können."
    )
    by_rule = query(f"""
        select source as Quelle, rule as Regel, count(*) as Zeilen
        from quality.dq_rejected_rows where observed_on > date '{since}'
        group by all order by Zeilen desc
    """)
    by_rule["Quelle"] = by_rule["Quelle"].map(SOURCE_LABELS)
    chart = (
        alt.Chart(by_rule)
        .mark_bar(size=16, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y("Regel:N", sort="-x", title=None, axis=alt.Axis(labelLimit=240)),
            x=alt.X("Zeilen:Q", title="Zeilen in Quarantäne", axis=alt.Axis(tickCount=5)),
            color=alt.Color(
                "Quelle:N",
                scale=alt.Scale(domain=list(SOURCE_LABELS.values()), range=SERIES[:3]),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=["Quelle:N", "Regel:N", "Zeilen:Q"],
        )
        .properties(height=alt.Step(30))
    )
    with_table(chart, by_rule)

    left, right = st.columns(2)
    with left:
        st.subheader("Fehlende Dateien")
        if missing.empty:
            st.markdown(f"{STATUS_ICON['ok']} In Ordnung: Alle Versanddateien sind angekommen.")
        else:
            for row in missing.itertuples():
                st.markdown(
                    f"{STATUS_ICON['knapp']} Beobachten: {row.expected_file} fehlt "
                    f"({row.missing_date:%d.%m.%Y})"
                )
    with right:
        st.subheader("Letzte Ladeläufe")
        st.dataframe(
            query("""
                select source as Quelle, finished_at as Ende, rows_received as Empfangen,
                       rows_new as Neu, detail as Detail
                from meta.load_log order by finished_at desc limit 8
            """),
            hide_index=True,
            width="stretch",
            column_config={
                "Ende": st.column_config.DatetimeColumn(format="DD.MM.YYYY HH:mm"),
                "Empfangen": st.column_config.NumberColumn(format="localized"),
                "Neu": st.column_config.NumberColumn(format="localized"),
            },
        )
