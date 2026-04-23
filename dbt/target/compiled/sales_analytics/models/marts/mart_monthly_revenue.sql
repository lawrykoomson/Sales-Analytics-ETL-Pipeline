/*
  Mart Model: mart_monthly_revenue
  ==================================
  Aggregates sales revenue by month.
  Powers revenue trend charts in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from "sales_warehouse"."sales_dw_staging"."stg_sales"

),

monthly as (

    select
        sale_year,
        sale_month,
        quarter_label,

        to_char(to_date(sale_month::text, 'MM'), 'Month') as month_name,

        count(transaction_id)                           as total_transactions,
        count(distinct customer_id)                     as unique_customers,
        sum(quantity)                                   as total_units_sold,

        round(sum(gross_revenue_ghs), 2)                as total_gross_revenue_ghs,
        round(sum(discount_amount_ghs), 2)              as total_discounts_ghs,
        round(sum(net_revenue_ghs), 2)                  as total_net_revenue_ghs,
        round(sum(gross_profit_ghs), 2)                 as total_gross_profit_ghs,
        round(avg(profit_margin_pct), 2)                as avg_profit_margin_pct,
        round(avg(net_revenue_ghs), 2)                  as avg_transaction_value_ghs,

        count(case when is_weekend then 1 end)          as weekend_transactions,
        count(case when is_high_value then 1 end)       as high_value_transactions,
        count(case when is_discounted then 1 end)       as discounted_transactions,

        round(
            sum(net_revenue_ghs)
            / sum(sum(net_revenue_ghs)) over () * 100
        , 2)                                            as revenue_share_pct

    from staged
    group by sale_year, sale_month, quarter_label

)

select * from monthly
order by sale_year, sale_month