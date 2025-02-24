def model(dbt,session):

    customers_df = dbt.source("raw_qwt","Customers")
    return customers_df