
  
    

  create  table "sales_warehouse"."sales_dw_marts"."mart_sales_by_channel__dbt_tmp"
  
  
    as
  
  (
    /*
  Mart Model: mart_sales_by_channel
  ===================================
  Aggregates sales KPIs by sales channel.
  Powers channel performance in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from "sales_warehouse"."sales_dw_staging"."stg_sales"

),

channel_sales as (

    select
        channel,

        count(transaction_id)                           as total_transactions,
        count(distinct customer_id)                     as unique_customers,
        sum(quantity)                                   as total_units_sold,

        round(sum(net_revenue_ghs), 2)                  as total_net_revenue_ghs,
        round(sum(gross_profit_ghs), 2)                 as total_gross_profit_ghs,
        round(avg(profit_margin_pct), 2)                as avg_profit_margin_pct,
        round(avg(net_revenue_ghs), 2)                  as avg_transaction_value_ghs,

        count(case when payment_method = 'MoMo'
                   then 1 end)                          as momo_payments,
        count(case when payment_method = 'Cash'
                   then 1 end)                          as cash_payments,
        count(case when payment_method = 'Card'
                   then 1 end)                          as card_payments,

        count(case when is_high_value then 1 end)       as high_value_transactions,
        count(case when is_discounted then 1 end)       as discounted_transactions,
        count(case when is_weekend then 1 end)          as weekend_transactions,

        round(
            sum(net_revenue_ghs)
            / sum(sum(net_revenue_ghs)) over () * 100
        , 2)                                            as revenue_share_pct

    from staged
    group by channel

)

select * from channel_sales
order by total_net_revenue_ghs desc
  );
  