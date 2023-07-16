# Puerto Rico COVID-19 data and analysis

This project, ongoing since May 2020, captures data about COVID-19 in
Puerto Rico and produces the analyses and visualizations that appear
in the following website (in Spanish only):

* https://covid-19-puerto-rico.org/


## Data capture

The [`downloader/`](downloader/) directory contains our data capture
and ingestion application, which carries out daily downloads of the
following data sources and ingests them into a data lake in Amazon S3:

1. Daily CSV downloads from the
   [Puerto Rico Department of Health's COVID-19 statistics dashboard](https://www.salud.pr.gov/estadisticas_v2);
2. Daily JSON downloads from the
   [Puerto Rico Department of Health's Biostatistics API](https://biostatistics.salud.pr.gov);
3. CSV downloads at diverse frequencies from
[the United States Deparment of Health and Human Services's HealthData.gov website](https://healthdata.gov/),
nd
[the United States' Centers for Disease Control's similar website](https://data.cdc.gov/).

In addition to that this project has collected a lot of data, much of
it copied by hand, from these older sources:

* The
  [Puerto Rico Department of Health's old reports on COVID-19 cases](http://www.salud.pr.gov/Estadisticas-Registros-y-Publicaciones/Pages/COVID-19.aspx),
  which were published from 2020 to 2021, later replaced by a CSV
  download feature.  These were only published in PDF format; this
  repository collects all of the PDFs under
  [`website/assets/source_material/pdf/`](website/assets/source_material/pdf/),
  and CSV files of painfully hand-extracted and validated data from
  them under [`website/assets/data/`](website/assets/data/).
* The Puerto Rico Department of  Health's Bioportal API, which was the
  predecessor of the present-day  Biostatistics API (listed above) and
  contained substantially the same information.
* Hospitalizations data from the
  [COVID Tracking Project](https://covidtracking.com/), which in turn
  collected them from the Puerto Rico Department of Health and the
  United States Health and Human Services Department.
* Misceallaneous sources like press reports or reports and graphs from
  the Puerto Rico Department of health that do not appear in these
  links but which journaklists and other parties shared with me.


## Data cleansing and analysis

The bulk of the data cleansing and analysis is built in SQL as a
[DBT](https://www.getdbt.com/) project that uses
[Amazon Athena](https://aws.amazon.com/athena/) as its backend.  The
code for that is under this directory:

* [`athena-dbt/`](athena-dbt/)


## Static website generator

The website is 100% static pages (there is no code running in any
HTTP servers, just HTML and client-side Javascript).  The application
that generates the pages is under this directory:

* [`website/`](website/)

The charts are made with the excellent
[Vega-Lite](https://vega.github.io/vega-lite/) framework and its
Python interface [Vega-Altair](https://altair-viz.github.io/).


## Acknowledgements

Robby Cortés (@RobbyCortes on Twitter) y Angélica Serrano-Román
(@angelicaserran0) who in the early days of the pandemic diligently
published every morning the Puerto Rico Department of Health's case
bulletins.

The Puerto Rico Public Health Trust for also facilitating the
publication of important government data early on.

Danilo Pérez and Dr. Rafael Irizarry for many suggestions and valuable
information.

