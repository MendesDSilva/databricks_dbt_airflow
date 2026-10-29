
{{ config(
    unique_key=['moeda', 'data_hora_cotacao']
) }}

WITH cotacoes AS (

    SELECT
        CAST(cotacaoCompra AS DECIMAL(18,6)) AS valor_cotacao,
        CAST(dataHoraCotacao AS TIMESTAMP) AS data_hora_cotacao,
        UPPER(TRIM(moeda)) AS moeda

    FROM {{ source('landingzone', 'src_cotacoes') }}

)

SELECT
    moeda,
    data_hora_cotacao,
    valor_cotacao
FROM cotacoes
