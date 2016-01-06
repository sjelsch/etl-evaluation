SELECT
  SUM(SUM_REVENUE) AS REVENUE
FROM FACTS
INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY)
WHERE
  LO_ORDERDATEYEARMONTHNUMLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYearMonthNum199401>'
  AND LO_DISCOUNT BETWEEN 4.0 AND 6.0
  AND LO_QUANTITY BETWEEN 26.0 AND 35.0
