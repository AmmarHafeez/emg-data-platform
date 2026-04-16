with source_data as (
    select *
    from {{ source('raw', 'emg_signals') }}
),

standardized as (
    select
        trim(subject_id) as subject_id,
        trim(session_id) as session_id,
        cast(timestamp as timestamp) as recorded_at,
        cast(channel_1 as double precision) as emg_channel_1,
        cast(channel_2 as double precision) as emg_channel_2,
        cast(channel_3 as double precision) as emg_channel_3,
        cast(channel_4 as double precision) as emg_channel_4,
        cast(channel_5 as double precision) as emg_channel_5,
        cast(channel_6 as double precision) as emg_channel_6,
        cast(channel_7 as double precision) as emg_channel_7,
        cast(channel_8 as double precision) as emg_channel_8,
        lower(trim(gesture_label)) as gesture_label,
        cast(ingested_at as timestamp) as ingested_at
    from source_data
)

select *
from standardized
