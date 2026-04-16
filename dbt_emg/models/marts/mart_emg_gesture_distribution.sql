with staged_emg as (
    select *
    from {{ ref('stg_emg_raw') }}
),

gesture_totals as (
    select
        gesture_label,
        count(*) as sample_count,
        count(distinct subject_id) as subject_count
    from staged_emg
    group by 1
)

select
    gesture_label,
    sample_count,
    subject_count,
    sample_count::numeric / nullif(sum(sample_count) over (), 0) as proportion_of_total
from gesture_totals
order by sample_count desc, gesture_label
