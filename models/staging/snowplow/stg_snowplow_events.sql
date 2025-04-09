with source as (
        select * from {{ source('snowplow', 'events') }}
  ),
  renamed as (
      select *
      from source
  )
  select * from renamed
    