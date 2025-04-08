


{{ config(materialized = 'incremental',  incremental_strategy = 'merge' , unique_key = 'order_id')}}

SELECT order_id,
  {{ dbt_utils.pivot('payment_method', dbt_utils.get_column_values( ref("stg_stripe__payments"),
                                                              'payment_method'), then_value= 'payment_amount', suffix = '_amount')
                }}
 {{ audit_columns() }}
 FROM {{ ref("stg_stripe__payments") }}
 where payment_status = 'success'
  {% if is_incremental() %}
    and update_at > ( Select max(update_at) from {{ this }})
  {% endif %}
 GROUP BY 1


