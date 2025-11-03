WITH expenses_customer_names AS (
    SELECT DISTINCT
          NULLIF(TRIM(party_name), '') AS party_name
    FROM {{ ref('stg_tally_expenses_data') }}
    WHERE party_name IS NOT NULL
)

, voucher_customer_address AS (
    SELECT DISTINCT
          NULLIF(TRIM(customer_name), '')    AS customer_name
        , NULLIF(TRIM(address_line_1), '')   AS address_line_1
        , NULLIF(TRIM(address_line_2), '')   AS address_line_2
        , NULLIF(TRIM(address_line_3), '')   AS address_line_3
        , NULLIF(TRIM(address_line_4), '')   AS address_line_4
        , NULLIF(TRIM(address_line_5), '')   AS address_line_5
    FROM {{ ref('stg_tally_voucher_data') }}
    WHERE customer_name IS NOT NULL
)

, source_data AS (
    SELECT DISTINCT
          e.party_name
        , v.address_line_1
        , v.address_line_2
        , v.address_line_3
        , v.address_line_4
        , v.address_line_5
    FROM expenses_customer_names AS e
    INNER JOIN voucher_customer_address AS v
        ON LOWER(TRIM(e.party_name)) = LOWER(TRIM(v.customer_name))
)

-- 1) word_tags por cada address_line + address_full_original (solo el texto unido)
, enriched_base AS (
    SELECT DISTINCT
          party_name
        , address_line_1
        , address_line_2
        , address_line_3
        , address_line_4
        , address_line_5
        , {{ word_tags("address_line_1") }} AS address_line_1_word_tags
        , {{ word_tags("address_line_2") }} AS address_line_2_word_tags
        , {{ word_tags("address_line_3") }} AS address_line_3_word_tags
        , {{ word_tags("address_line_4") }} AS address_line_4_word_tags
        , {{ word_tags("address_line_5") }} AS address_line_5_word_tags
        , TRIM(CONCAT_WS(' '
              , COALESCE(address_line_1, '')
              , COALESCE(address_line_2, '')
              , COALESCE(address_line_3, '')
              , COALESCE(address_line_4, '')
              , COALESCE(address_line_5, '')
          )) AS address_full_original
    FROM source_data
)

-- 1.b) ahora sí, calculo word_tags de la dirección completa usando el alias previo
, enriched AS (
    SELECT
          eb.*
        , {{ word_tags("address_full_original") }} AS address_full_word_tags
    FROM enriched_base AS eb
)

-- 2) Normalizo líneas a filas con prioridad 5 -> 1
, lines AS (
    SELECT party_name, 5 AS line_no, address_line_5 AS line_txt, address_line_5_word_tags AS line_tags FROM enriched
    UNION ALL
    SELECT party_name, 4, address_line_4, address_line_4_word_tags FROM enriched
    UNION ALL
    SELECT party_name, 3, address_line_3, address_line_3_word_tags FROM enriched
    UNION ALL
    SELECT party_name, 2, address_line_2, address_line_2_word_tags FROM enriched
    UNION ALL
    SELECT party_name, 1, address_line_1, address_line_1_word_tags FROM enriched
)

-- 3) catálogo de países desde stg_adhoc_country_state (DISTINCT por país)
, state_country_dim AS (
    SELECT DISTINCT
          LOWER(TRIM(country_en)) AS country_en_lc
        , ANY_VALUE(country_code) AS country_code
        , MAX(country_en)         AS country_en
    FROM {{ ref('stg_adhoc_country_state') }}
    WHERE country_en IS NOT NULL
    GROUP BY LOWER(TRIM(country_en))
)

-- 4) POSITION de cada address_line_*_word_tags contra country_en (solo por country_en)
, country_hits AS (
    SELECT
          l.party_name
        , l.line_no
        , l.line_txt
        , l.line_tags
        , sc.country_code
        , sc.country_en
        , CASE WHEN POSITION(sc.country_en_lc IN LOWER(l.line_tags)) > 0 THEN 1 ELSE 0 END AS hit
    FROM lines AS l
    LEFT JOIN state_country_dim AS sc
      ON POSITION(sc.country_en_lc IN LOWER(l.line_tags)) > 0
)

-- 5) Primer match por prioridad (línea 5 → 1)
, best_country AS (
    SELECT
          party_name
        , line_no        AS matched_line_no
        , country_code
        , country_en
    FROM country_hits
    WHERE hit = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY party_name
        ORDER BY line_no DESC
    ) = 1
)

-- 6) salida final (¡ojo! sin coma previa)
SELECT DISTINCT
      e.party_name AS legal_name
    , {{ literal_name("e.party_name") }}    AS tally_literal_name
    , e.address_line_1
    , e.address_line_1_word_tags
    , e.address_line_2
    , e.address_line_2_word_tags
    , e.address_line_3
    , e.address_line_3_word_tags
    , e.address_line_4
    , e.address_line_4_word_tags
    , e.address_line_5
    , e.address_line_5_word_tags
    , e.address_full_original
    , e.address_full_word_tags
    , bc.country_code
    , bc.country_en
    , bc.matched_line_no AS matched_from_line
FROM enriched AS e
LEFT JOIN best_country AS bc
  ON bc.party_name = e.party_name
