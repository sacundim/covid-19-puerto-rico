# Census data tables

* `municipal_population` is total population for Puerto Rico's municipalities,
  and has both 2020 Census and 2019 Population Estimates Program values.
* `acs_2019_1y_age_ranges` is Puerto Rico popluation by age from 
  2019 American Community Survey 1-Year Estimates (TableID S0101)

The rest of the tables are relationship/dimension tables to join 
various others to this and perform aggregations:

* `age_range_reln`: The relationship table that joins all of the
  others together.
* `bioportal_age_ranges`: Join to Bioportal's `age_range` field
* `prdoh_age_ranges`: Join to my capture of Puerto Rico Dept.
  of Health daily reports
* `municipal_abbreviations` is three-letter codes for municipalities
  from [`gis.pr.gov`](https://gis.pr.gov/descargaGeodatos/GeografiaCensal/Pages/ABREVIATURAS-DE-MUNICIPIOS.aspx).