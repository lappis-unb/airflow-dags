{{
    config(
        materialized="table",
        indexes=[
            {
                "columns": ["component_id", "period", "date"],
            }
        ],
        meta={
            "datasets_trigger": "bronze_devices_detection_get_type"
        }
    )
}}


select
    split_part(devices."url", '/', 7) as component_id,
    devices."period",
    devices."date",
    devices."space",
    devices."label" as device_type,
    devices.nb_visits
from {{ source("matomo", "devices_detection_get_type") }} devices
