{{ config(schema="a51_finance", materialized="table") }}

SELECT '{{ var("UNI_TIME", "hi")}}' AS temp_c