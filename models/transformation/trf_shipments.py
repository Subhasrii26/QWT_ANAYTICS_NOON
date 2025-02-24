import snowflake.snowpark.functions as F 

def model(dbt, session):
    dbt.config(materialized = 'table')
    shipments_df = dbt.ref('shipments_snapshot')
    shippers_df  = dbt.ref('shippers')

    fin_shipments_df = (
        shipments_df
        .join(shippers_df,shipments_df.shipperID == shippers_df.shipperID,'left')
        .select(shipments_df.orderid.alias('orderid'),
                shipments_df.lineno.alias('lineno'),
                shippers_df.companyname.alias('companyname'),
                shipments_df.shipmentdate.alias('shipmentdate'),
                shipments_df.status.alias('status'),
                shipments_df.dbt_valid_to.alias('dbt_valid_to')
        )
    )

    fin_shipments_df = fin_shipments_df.filter(F.col('dbt_valid_to').isNull())
    return fin_shipments_df

