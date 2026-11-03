"""Dashboard for the retail data pipeline. Start with `streamlit run dashboard/app.py`."""

import os

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from pipeline.warehouse import WAREHOUSE_PATH

# Categorical slots in a fixed order (validated for colour-vision deficiencies).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
STATUS = {"ok": "#0ca30c", "knapp": "#fab219", "kritisch": "#ec835a", "ausverkauft": "#d03b3b"}
STATUS_ICON = {"ok": "●", "knapp": "▲", "kritisch": "◆", "ausverkauft": "■"}
CHANNEL_LABELS = {"webshop": "Webshop", "app": "App", "marketplace": "Marktplatz"}
SOURCE_LABELS = {"shop": "Shop", "logistics": "Logistik", "warehouse": "Lager"}

st.set_page_config(page_title="Handelsdaten · Nordhafen Living", page_icon="📦", layout="wide")


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


def percent(value: float) -> str:
    return f"{value * 100:.1f} %".replace(".", ",")


# Vega-Lite formats numbers the US way, so axis labels swap the separator explicitly.
GERMAN_THOUSANDS = "replace(format(datum.value, ',.0f'), ',', '.')"


def decimal(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def thin_bars(chart: alt.Chart) -> alt.Chart:
    return chart.mark_bar(size=14, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)


def with_table(chart: alt.Chart, table: pd.DataFrame) -> None:
    st.altair_chart(chart, use_container_width=True)
    with st.expander("Als Tabelle anzeigen"):
        st.dataframe(table, hide_index=True, use_container_width=True)


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

months = st.sidebar.select_slider(
    "Zeitraum", options=[1, 3, 6, 12], value=12, format_func=lambda m: f"{m} Monate"
)
latest = query("select max(order_date) as d from marts.agg_daily_sales")["d"][0]
since = (pd.Timestamp(latest) - pd.DateOffset(months=months)).date()
period = f"order_date > date '{since}'"

st.title("Nordhafen Living · Handelsdaten")
last_load = query("select max(finished_at) as t from meta.load_log")["t"][0]
st.caption(
    f"Daten bis {pd.Timestamp(latest):%d.%m.%Y} · "
    f"letzter Ladelauf {pd.Timestamp(last_load):%d.%m.%Y %H:%M} · "
    f"Quellen: Shop-API, Warenwirtschaft, Logistikpartner (CSV)"
)

kpi = query(f"""
    select sum(revenue) as revenue, sum(orders) as orders,
           sum(returned_orders) / sum(orders) as return_rate
    from marts.agg_daily_sales where {period}
""").iloc[0]
on_time = query(f"""
    select avg(is_on_time::integer) as rate from marts.fct_orders
    where {period} and delivered_at is not null
""")["rate"][0]
rejected = query(
    f"select count(*) as n from quality.dq_rejected_rows where observed_on > date '{since}'"
)["n"][0]
missing = query("select * from quality.dq_missing_files order by missing_date")

cols = st.columns(5)
cols[0].metric("Umsatz", euro(kpi["revenue"]))
cols[1].metric("Bestellungen", f"{int(kpi['orders']):,}".replace(",", "."))
cols[2].metric("Retourenquote", percent(kpi["return_rate"]))
cols[3].metric("Pünktlich zugestellt", percent(on_time), help="Zustellung innerhalb von 5 Tagen")
cols[4].metric("Zeilen in Quarantäne", f"{rejected:,}".replace(",", "."))

sales_tab, delivery_tab, stock_tab, returns_tab, quality_tab = st.tabs(
    ["Umsatz", "Lieferung", "Lager", "Retouren", "Datenqualität"]
)

with sales_tab:
    weekly = query(f"""
        select date_trunc('week', order_date)::date as woche, channel, sum(revenue) as umsatz
        from marts.agg_daily_sales where {period} group by all order by 1
    """)
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
    with_table(chart, weekly[["woche", "Kanal", "Umsatz"]])

    st.subheader("Bestseller")
    top = query(f"""
        select product_name as Artikel, category as Kategorie, sum(quantity) as Stück,
               round(sum(revenue)) as Umsatz,
               round(sum(margin) / sum(revenue) * 100, 1) as "Marge %"
        from marts.fct_order_lines where {period} and status <> 'cancelled'
        group by all order by Umsatz desc limit 10
    """)
    st.dataframe(
        top,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Stück": st.column_config.NumberColumn(format="localized"),
            "Umsatz": st.column_config.NumberColumn(format="euro"),
            "Marge %": st.column_config.NumberColumn(format="%.1f %%"),
        },
    )

with delivery_tab:
    perf = query(f"""
        select week as woche, carrier as Paketdienst, on_time_rate as puenktlich,
               avg_days_to_deliver as tage, delivered_orders as zugestellt
        from marts.agg_delivery_performance
        where week > date '{since}' and delivered_orders > 20 order by 1
    """)
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
                legend=alt.Legend(orient="top", title=None),
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
    st.caption(
        "Versprechen an Kunden: Zustellung innerhalb von 5 Tagen nach Bestellung. "
        "Im Dezember sind alle Paketdienste überlastet."
    )
    with_table(chart, perf[["woche", "Paketdienst", "Pünktlich", "Ø Tage", "zugestellt"]])

with stock_tab:
    stock = query("""
        select sku, product_name, category, on_hand, avg_daily_units, days_of_cover, coverage_status
        from marts.agg_stock_coverage
        order by case coverage_status when 'ausverkauft' then 0 when 'kritisch' then 1
                 when 'knapp' then 2 else 3 end, days_of_cover
    """)
    counts = stock["coverage_status"].value_counts()
    status_cols = st.columns(4)
    for col, status in zip(status_cols, ["ausverkauft", "kritisch", "knapp", "ok"], strict=True):
        col.markdown(
            f"<span style='color:{STATUS[status]};font-size:18px'>{STATUS_ICON[status]}</span> "
            f"**{counts.get(status, 0)}** {status}",
            unsafe_allow_html=True,
        )
    stock["Status"] = stock["coverage_status"].map(lambda s: f"{STATUS_ICON[s]} {s}")
    st.dataframe(
        stock.rename(
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
        use_container_width=True,
        column_config={
            "Bestand": st.column_config.NumberColumn(format="localized"),
            "Ø Absatz/Tag": st.column_config.NumberColumn(format="localized"),
            "Reichweite (Tage)": st.column_config.NumberColumn(format="localized"),
        },
    )
    st.caption(
        "Reichweite = Bestand geteilt durch den durchschnittlichen Absatz der letzten 28 Tage."
    )

with returns_tab:
    returns = query("""
        select category as Kategorie, return_rate as quote, returned_orders, shipped_orders,
               top_reason
        from marts.agg_returns order by quote desc
    """)
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
    labels = chart.mark_text(align="left", dx=6, color="#52514e").encode(text="Quote:N")
    with_table(
        chart + labels,
        returns[["Kategorie", "Quote", "returned_orders", "shipped_orders", "top_reason"]],
    )

    reasons = query(f"""
        select return_reason as Grund, count(*) as Retouren
        from marts.fct_orders where is_returned and {period} group by 1 order by 2 desc
    """)
    st.dataframe(reasons, hide_index=True, use_container_width=True)

with quality_tab:
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
            st.success("Alle Versanddateien sind angekommen.")
        else:
            for row in missing.itertuples():
                st.warning(f"■ {row.expected_file} fehlt ({row.missing_date:%d.%m.%Y})")
    with right:
        st.subheader("Letzte Ladeläufe")
        st.dataframe(
            query("""
                select source as Quelle, finished_at as Ende, rows_received as Empfangen,
                       rows_new as Neu, detail as Detail
                from meta.load_log order by finished_at desc limit 8
            """),
            hide_index=True,
            use_container_width=True,
        )
