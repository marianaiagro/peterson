--table name
--owner
--when was created
--brief description about the table

WITH source_data AS (

    SELECT
        *
    FROM {{ source('icu', 'organization') }} AS organization

)

SELECT
    *
FROM source_data


--buenas practicas sql/dbt

    --espacios para igualdades

    --coma antes de la seleccion de nuevas columas
    --(ej: SELECT
    --      col1
    --      ,col2
    --    FROM table;)

    --sentencias sql en mayuscula
    --columnas o nombres de tablas en minuscula

    --seleccionar palabras completas para nombre de tabla o columnas

    --iniciar las vistas de la carpeta staging con el acronimo 'stg'

    --podriamos incluir un readme en las vitas o tablas 

    --iniciar las vistas de la carpeta intermediate con el acronimo 'int'

    --los archivos de la carpeta modeled no contienen prefijo

    --NO USAR MAYUSCULAS EN ARCHIVOS DE DBT