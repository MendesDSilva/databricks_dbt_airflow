{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

-- Identifica a primeira data disponível para cada moeda
WITH base_dados AS (

    SELECT
        moeda,
        MIN(CAST(data_hora_cotacao AS DATE)) AS min_data

    FROM {{ ref('stg_cotacoes') }}

    GROUP BY moeda

),

-- Gera todas as datas entre a primeira cotação e a data atual
datas_periodo AS (

    SELECT
        moeda,

        EXPLODE(
            SEQUENCE(
                min_data,
                CURRENT_DATE(),
                INTERVAL 1 DAY
            )
        ) AS data

    FROM base_dados

),

-- Consolida as cotações por moeda e dia
cotacoes_diarias AS (

    SELECT
        moeda,
        CAST(data_hora_cotacao AS DATE) AS data,
        valor_cotacao,

        ROW_NUMBER() OVER (
            PARTITION BY
                moeda,
                CAST(data_hora_cotacao AS DATE)

            ORDER BY data_hora_cotacao DESC
        ) AS rn

    FROM {{ ref('stg_cotacoes') }}

),

-- Combina o calendário com as cotações existentes
dados_completos AS (

    SELECT
        dp.moeda,
        dp.data,
        cd.valor_cotacao AS cotacao

    FROM datas_periodo dp

    LEFT JOIN cotacoes_diarias cd
        ON dp.moeda = cd.moeda
        AND dp.data = cd.data
        AND cd.rn = 1

),

-- Preenche dias sem cotação com a última cotação válida
fact_cotacoes_final AS (

    SELECT
        moeda,
        data,

        LAST_VALUE(cotacao, TRUE) OVER (
            PARTITION BY moeda
            ORDER BY data
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW
        ) AS cotacao

    FROM dados_completos

)

SELECT
    {{ generate_surrogate_key(['moeda', 'data']) }} AS cotacao_id,
    moeda,
    data,
    cotacao,
    {{ audit_columns() }}

FROM fact_cotacoes_final