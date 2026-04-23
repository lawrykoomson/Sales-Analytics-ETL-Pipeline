/*
  Mart Model: mart_sales_by_category
  ====================================
  Aggregates sales KPIs by product category.
  Powers category performance in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_sales') }}

),

category_sales as (

    select
        category,

        count(transaction_id)                           as total_transactions,
        count(distinct customer_id)                     as unique_customers,
        count(distinct product_id)                      as unique_products,
        sum(quantity)                                   as total_units_sold,

        round(sum(gross_revenue_ghs), 2)                as total_gross_revenue_ghs,
        round(sum(discount_amount_ghs), 2)              as total_discounts_ghs,
        round(sum(net_revenue_ghs), 2)                  as total_net_revenue_ghs,
        round(sum(gross_profit_ghs), 2)                 as total_gross_profit_ghs,
        round(avg(profit_margin_pct), 2)                as avg_profit_margin_pct,
        round(avg(net_revenue_ghs), 2)                  as avg_transaction_value_ghs,
        round(avg(unit_price_ghs), 2)                   as avg_unit_price_ghs,

        count(case when is_high_value then 1 end)       as high_value_transactions,
        count(case when is_discounted then 1 end)       as discounted_transactions,

        round(
            sum(net_revenue_ghs)
            / sum(sum(net_revenue_ghs)) over () * 100
        , 2)                                            as revenue_share_pct

    from staged
    group by category

)

select * from category_sales
order by total_net_revenue_ghs desc