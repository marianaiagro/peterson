
/*
Purpose:
if the column contains brackets ([...]), this removes everything after the first “[”.
*/

{% macro _strip_after_bracket(col) -%}
    case
        when charindex('[', {{ col }}) > 0
            then substr({{ col }}, 1, charindex('[', {{ col }}) - 1)
        else {{ col }}
    end
{%- endmacro %}
