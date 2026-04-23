
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_revenue_ghs
from "sales_warehouse"."sales_dw_staging"."stg_sales"
where net_revenue_ghs is null



  
  
      
    ) dbt_internal_test