/*
Purpose:
Generates a simplified company name by removing text after “[”, 
converting to lowercase, and stripping spaces and punctuation.
*/ 

{%- macro literal_name(col) -%}
  lower(
    translate(
      {{ _strip_after_bracket(col) }},
      ' .,-_()',
      ''
    )
  )
{%- endmacro -%}
