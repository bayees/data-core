{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/location_tracking/dim_location.parquet') 
}}

with home_assistant_positions as (

    select * from {{ source('home_assistant', 'states__value') }}

), 

dawa_adresses as (

   select 
        _locations_latitude_degrees as latitude_degrees,
        _locations_longitude_degrees as longitude_degrees,
        adressebetegnelse,
        vejstykke__navn as vejstykke_navn,
        husnr,
        postnummer__nr as postnummer_nr,
        postnummer__navn as postnummer_navn,
        adgangspunkt__h_jde as adgangspunkt_højde,
        sogn__navn as sogn_navn,
        region__navn as region_navn,
        landsdel__navn as landsdel_navn,
        retskreds__navn as retskreds_navn,
        politikreds__navn as politikreds_navn,
        storkreds__navn as storkreds_navn,
        valglandsdel__navn as valglandsdel_navn
   from {{ source('dawa', 'dawa_locations') }}
   
),

dawa_enriched_locations as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['home_assistant_positions.attributes__latitude', 'home_assistant_positions.attributes__longitude']) }} as location_id

        ,home_assistant_positions.attributes__latitude as latitude_degrees 
        ,home_assistant_positions.attributes__longitude as longitude_degrees

        ,case 
            when dawa_adresses.latitude_degrees is null
                and dawa_adresses.longitude_degrees is null
            then 0
            else 1
        end as dawa_enriched
        ,dawa_adresses.adressebetegnelse                                    AS dawa_adressebetegnelse
        ,dawa_adresses.vejstykke_navn                                     AS dawa_vejstykke_navn
        ,dawa_adresses.husnr                                                AS dawa_husnummer
        ,dawa_adresses.postnummer_nr                                      AS dawa_postnummer_nr
        ,dawa_adresses.postnummer_navn                                    AS dawa_postnummer_navn
        ,dawa_adresses.adgangspunkt_højde                                 AS dawa_højde
        ,dawa_adresses.sogn_navn                                          AS dawa_sogn_navn
        ,dawa_adresses.region_navn                                        AS dawa_region_navn
        ,dawa_adresses.landsdel_navn                                      AS dawa_landsdel_navn
        ,dawa_adresses.retskreds_navn                                     AS dawa_retskreds_navn
        ,dawa_adresses.politikreds_navn                                   AS dawa_politikreds_navn
        ,dawa_adresses.storkreds_navn                                     AS dawa_storkreds_navn
        ,dawa_adresses.valglandsdel_navn                                  AS dawa_valglandsdel_navn
        ,home_assistant_positions.state                                   AS location_of_interest
    FROM home_assistant_positions
    LEFT JOIN dawa_adresses
      ON home_assistant_positions.attributes__latitude = dawa_adresses.latitude_degrees
      AND home_assistant_positions.attributes__longitude = dawa_adresses.longitude_degrees
)

select *
from dawa_enriched_locations