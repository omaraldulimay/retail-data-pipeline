from dagster import AssetKey


def test_definitions_import_and_asset_graph():
    from orchestration.definitions import defs

    keys = defs.resolve_asset_graph().get_all_asset_keys()
    expected_raw = {
        AssetKey(["raw", "shop_products"]),
        AssetKey(["raw", "shop_orders"]),
        AssetKey(["raw", "wms_stock_snapshots"]),
        AssetKey(["raw", "logistics_events"]),
        AssetKey(["meta", "loaded_files"]),
    }
    expected_marts = {
        AssetKey(["marts", name])
        for name in (
            "fct_orders",
            "fct_order_lines",
            "fct_shipment_events",
            "agg_daily_sales",
            "agg_delivery_performance",
            "agg_returns",
            "agg_stock_coverage",
        )
    }
    assert expected_raw <= keys
    assert expected_marts <= keys


def test_daily_pipeline_job_and_schedule():
    from orchestration.definitions import defs

    assert defs.resolve_job_def("daily_pipeline").name == "daily_pipeline"
    schedule = defs.resolve_schedule_def("daily_pipeline_schedule")
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "Europe/Berlin"
    assert schedule.job_name == "daily_pipeline"


def test_reload_job_is_partitioned_and_daily_pipeline_is_not():
    from orchestration.definitions import defs

    reload_job = defs.resolve_job_def("reload_logistics_day")
    assert reload_job.partitions_def is not None
    assert reload_job.partitions_def.start.date().isoformat() == "2025-11-01"
    assert reload_job.partitions_def.end_offset == 0
    assert defs.resolve_job_def("daily_pipeline").partitions_def is None


def test_new_asset_checks_are_registered():
    from orchestration.definitions import defs

    names = {key.name for key in defs.resolve_asset_graph().asset_check_keys}
    assert {"sources_fresh", "latest_day_not_anomalous"} <= names
