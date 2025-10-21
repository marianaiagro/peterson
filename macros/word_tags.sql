
/* 
Processing pipeline:
1) remove text after “[”
2) normalize accents
3) lowercase + trim
4) remove punctuation
5) replace spaces with commas
*/

{% macro word_tags(col) -%}
    replace(
      trim(
        lower(
          translate(
            {{ _remove_accents(_strip_after_bracket(col)) }},
            '.,-_()/]',
            ''
          )
        )
      ),
      ' ',
      ','
    )
{%- endmacro %}
