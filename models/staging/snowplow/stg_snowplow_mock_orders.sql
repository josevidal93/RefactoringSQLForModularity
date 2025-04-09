with source as (
        select * from {{ source('snowplow', 'mock_orders') }}
  ),
  renamed as (
      select *
          

      from source
  )
  select * from renamed
    