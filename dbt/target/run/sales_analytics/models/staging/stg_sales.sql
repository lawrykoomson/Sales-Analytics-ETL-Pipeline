
  create view "sales_warehouse"."sales_dw_staging"."stg_sales__dbt_tmp"
    
    
  as (
    /*
  Staging Model: stg_sales
  ==========================
  Cleans and standardises raw sales fact table.
  Single source of truth for all downstream sales models.

  Source: sales_dw.fact_sales
  Author: Lawrence Koomson
*/

with source as (

    select * from "sales_warehouse"."sales_dw"."fact_sales"

),

staged as (

    select
        transaction_id,
        timestamp                                       as transaction_timestamp,
        sale_date,
        sale_month,
        sale_quarter,
        sale_year,
        sale_hour,
        day_of_week,
        is_weekend,

        upper(trim(customer_id))                        as customer_id,
        upper(trim(product_id))                         as product_id,
        initcap(trim(product_name))                     as product_name,
        initcap(trim(category))                         as category,
        initcap(trim(region))                           as region,
        initcap(trim(channel))                          as channel,
        initcap(trim(payment_method))                   as payment_method,
        upper(trim(salesperson_id))                     as salesperson_id,
        upper(trim(store_id))                           as store_id,

        unit_price_ghs,
        quantity,
        discount_pct,
        gross_revenue_ghs,
        discount_amount_ghs,
        net_revenue_ghs,
        cost_ghs,
        gross_profit_ghs,
        profit_margin_pct,

        is_high_value,
        is_discounted,
        revenue_tier,
        processed_at,

        case
            when sale_month in (1,2,3)   then 'Q1'
            when sale_month in (4,5,6)   then 'Q2'
            when sale_month in (7,8,9)   then 'Q3'
            else 'Q4'
        end                                             as quarter_label,

        case
            when sale_hour between 6  and 11 then 'Morning (6-12)'
            when sale_hour between 12 and 16 then 'Afternoon (12-17)'
            when sale_hour between 17 and 20 then 'Evening (17-21)'
            else 'Night (21+)'
        end                                             as time_of_day

    from source
    where
        transaction_id is not null
        and net_revenue_ghs > 0

)

select * from staged
  );