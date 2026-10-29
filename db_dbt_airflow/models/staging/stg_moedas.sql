{{ config(
    unique_key='moeda'
) }}


WITH source as (
    SELECT 
        UPPER(TRIM(NomeMoeda)) as nome_moeda
        ,UPPER(TRIM(Moeda)) as moeda


    FROM {{source('landingzone','src_moedas')}}
)

SELECT * FROM source

