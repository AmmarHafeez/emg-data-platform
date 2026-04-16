with staged_emg as (
    select *
    from {{ ref('stg_emg_raw') }}
),

sample_level_metrics as (
    select
        subject_id,
        session_id,
        gesture_label,
        (
            emg_channel_1 +
            emg_channel_2 +
            emg_channel_3 +
            emg_channel_4 +
            emg_channel_5 +
            emg_channel_6 +
            emg_channel_7 +
            emg_channel_8
        ) / 8.0 as sample_avg_signal,
        greatest(
            emg_channel_1,
            emg_channel_2,
            emg_channel_3,
            emg_channel_4,
            emg_channel_5,
            emg_channel_6,
            emg_channel_7,
            emg_channel_8
        ) as sample_max_signal
    from staged_emg
)

select
    subject_id,
    session_id,
    count(*) as row_count,
    avg(sample_avg_signal) as avg_signal,
    max(sample_max_signal) as max_signal,
    count(distinct gesture_label) as distinct_gestures
from sample_level_metrics
group by 1, 2
