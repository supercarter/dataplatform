

with source_data as (

    select  cast(VendorID as varchar(100)) as VendorID, 
            cast(tpep_pickup_datetime as varchar(100)) as tpep_pickup_datetime, 
            cast(tpep_dropoff_datetime as varchar(100)) as tpep_dropoff_datetime, 
            cast(passenger_count as varchar(100)) as passenger_count, 
            cast(trip_distance as varchar(100)) as trip_distance, 
            cast(RatecodeID as varchar(100)) as RatecodeID, 
            cast(store_and_fwd_flag as varchar(100)) as store_and_fwd_flag, 
            cast(PULocationID as varchar(100)) as PULocationID, 
            cast(DOLocationID as varchar(100)) as DOLocationID, 
            cast(payment_type as varchar(100)) as payment_type, 
            cast(fare_amount as varchar(100)) as fare_amount, 
            cast(extra as varchar(100)) as extra, 
            cast(mta_tax as varchar(100)) as mta_tax, 
            cast(tip_amount as varchar(100)) as tip_amount, 
            cast(tolls_amount as varchar(100)) as tolls_amount, 
            cast(improvement_surcharge as varchar(100)) as improvement_surcharge, 
            cast(total_amount as varchar(100)) as total_amount, 
            cast(congestion_surcharge as varchar(100)) as congestion_surcharge, 
            cast(airport_fee as varchar(100)) as airport_fee, 
            cast(file as varchar(100)) as file, 
            cast(mm as varchar(100)) as mm, 
            cast(yyyy as varchar(100)) as yyyy
            
    from {{source('stage','yellow_trip')}}

)

select *
from source_data
