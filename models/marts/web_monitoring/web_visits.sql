{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id'
) }}

with events as 
(
    select * from {{ ref('stg_snowplow_events')}}
    {% if(is_incremental() ) %}
        where collector_tstamp > (select max(last_visit) from {{ this}} )
    {% endif %}
)
, view_pages as 
(
    select * from events
    where event = 'page_view'
)
, agregation_page_view as
(
    select page_view_id,
           min( derived_tstamp) start_visit
           ,max(collector_tstamp) last_visit
           , count(1) *10 approx_time_on_page

    from events
    group by 1
)

select view_pages.*,
    start_visit,
    last_visit,
    approx_time_on_page
    {{ audit_columns()}}

from view_pages
inner join agregation_page_view
on view_pages.page_view_id = agregation_page_view.page_view_id