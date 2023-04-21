SELECT
    IF(device_code='0','Desktop','Mobile') AS device_type,
    IF(geo_country = 'gbr',1,0) AS flg_country_uk,
    IF(geo_country != 'gbr',local_currency,"GBP") AS currency,
    CASE WHEN SUM(CAST(split(formula,"=")[SAFE_OFFSET(1)] AS FLOAT64)) IS NULL THEN 0
         ELSE SUM(CAST(split(formula,"=")[SAFE_OFFSET(1)] AS FLOAT64))
    END AS formula_result
FROM geo_stuff.currency_formulas