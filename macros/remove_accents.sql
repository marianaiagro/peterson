/*
 Purpose:
 Chain multiple TRANSLATE calls to normalize accents and ñ/ç
*/

{% macro _remove_accents(col) -%}
    TRANSLATE(
      TRANSLATE(
        TRANSLATE(
          TRANSLATE(
            TRANSLATE(
              TRANSLATE(
                TRANSLATE(
                  {{ col }},
                  'áàäâãåāăąǎảạầấậẩẫằắặẳẵǻ',
                  'aaaaaaaaaaaaaaaaaaaa'
                ),
                'éèëêěēęẻẽẹếềệểễ',
                'eeeeeeeeeeeeeee'
              ),
              'íìïîīĩỉịǐ',
              'iiiiiiii'
            ),
            'óòöôõőōỏọơờớợởỡộổỗ',
            'oooooooooooooooo'
          ),
          'úùüûũūůűŭủụưứừựửữ',
          'uuuuuuuuuuuuuu'
        ),
        'ýỳŷÿỹỷỵ',
        'yyyyyyy'
      ),
      'ñç',
      'nc'
    )
{%- endmacro %}