import snowflake.snowpark.functions as F
 
def model(dbt, session):
 
    dbt.config(materialized = 'table', schema = 'reporting_dev')
 
    orders_df = dbt.ref('fct_orders')
 
    customers_df = dbt.ref('dim_customers')
 
    products_df = dbt.ref('dim_products')
 
    final_orders_df = (
        orders_df
        .join(customers_df, orders_df.customerid == customers_df.customerid, 'left')
        .join(products_df, orders_df.productid == products_df.productid, 'left')
        .select(customers_df.companyname,
                customers_df.contactname.alias('contactname'),
                orders_df.orderdate.alias('orderdate'),
                orders_df.orderid.alias('orderid'),
                orders_df.linesalesamount.alias('linesalesamount'),
                orders_df.quantity.alias('quantity'),
                orders_df.margin.alias('margin')
                )
    )
    return final_orders_df