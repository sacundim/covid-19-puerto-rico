# Datos y análisis del COVID-19 en Puerto Rico

Este proyecto recopila datos sobre COVID-19 publicados por el 
Departamento de Salud de Puerto Rico en un formato legible por
computadora, y contiene algunos análisis de estos.

Visita el dashboard en:

* https://covid-19-puerto-rico.org/


## Datos de fuente y archivos CSV

Los datos corrientes provienen principalmente de:
 
4. El [COVID Tracking Project](https://covidtracking.com/) (hospitalizaciones),
   que a su vez los obtuvo del Departamento de Salud de Puerto Rico y del Departamento de
   Salud y Servicios Humanos de los Estados Unidos.
5. Fuentes misceláneas como reportes de prensa o informes y gráficas del Departamento de 
   Salud de Puerto Rico que no aparecen en esos enlaces pero que se han compartido con 
   periodistas.


## Captura de datos

El subdirectorio [`downloader/`](downloader/) contiene la aplicación de captura
e ingesta de datos, que diariamente captura una selección de datos de estas fuentes
principales:

1. Descargas diarias del [tablero de estadísticas de COVID-19 del Departamento de Salud de Puerto Rico](https://www.salud.pr.gov/estadisticas_v2);
2. Descargas diarias del [API de Bioestadísticas del Departamento de Salud de Puerto Rico](https://biostatistics.salud.pr.gov).
3. Publicaciones de datos (principalmente hospitalarios) de [la página HealthData.gov del
   Departamento de Salud y Servicios Humanos de los Estados Unidos](https://healthdata.gov/)
   y de [la semejante página del CDC estadounidense](https://data.cdc.gov/);

Adicionalmente existe una colección de datos más viejos (muchos capturados a mano) de
estas fuentes:

* Los [informes PDF de Casos Positivos COVID-19](http://www.salud.pr.gov/Estadisticas-Registros-y-Publicaciones/Pages/COVID-19.aspx)
  del Departamento de Salud de Puerto Rico, publicados 2020 al 2021.  Aquí recogemos estos 
  informes en su formato PDF original bajo
  [`website/assets/source_material/pdf/`(website/assets/source_material/pdf/), 
  y archivos CSV de datos extraídos a mano de estos bajo [`website/assets/data/`](website/assets/data/).
* El API del Bioportal del Departamento de Salud de Puerto Rico, el predecesor del corriente
  de Bioestadísticas que tiene la misma información.
* Datos de hospitalizaciones del [COVID Tracking Project](https://covidtracking.com/),
  que a su vez los recopiló del Departamento de Salud de Puerto Rico y del Departamento de 
  Salud y Servicios Humanos de los Estados Unidos.
* Fuentes misceláneas como reportes de prensa o informes y gráficas del Departamento de 
  Salud de Puerto Rico que no aparecen en esos enlaces pero que se han compartido con 
  periodistas.


## Limpieza y análisis de datos

La limpieza y análisis de datos está construida en SQL como un proyecto de
[la herramienta DBT](https://www.getdbt.com/), y corre bajo 
[Amazon Athena](https://aws.amazon.com/athena/), un servicio de SQL 
en la nube. El código está en este directorio:

* [`athena-dbt/`](athena-dbt/)

Las visualizaciones finales generalmente hacen otros cálculos adicionales 
a los que hace el código aquí enlazado; especialmente, los cálculos de 
promedios móviles generalmente están en las visualizaciones en vez del SQL.


## Generador de páginas estáticas

Las páginas web son 100% estático (no hay ningún código que ejecute en servidores 
HTTP, solo HTML y Javascript en el cliente), y la aplicación que las generara se 
halla en este directorio:

* [`website/](website/)

Las gráficas son hechas con el excelente sistema [Vega-Lite](https://vega.github.io/vega-lite/)
y su interfac en Python [Vega-Altair](https://altair-viz.github.io/).


## Agradecimientos

A Robby Cortés (@RobbyCortes en Twitter) y Angélica Serrano-Román
(@angelicaserran0) que diligentemente publican los boletines del
Departamento de Salud todas las mañanas.

A Danilo Pérez por muchas sugerencias valiosas.

Al Fideicomiso de Salud de Puerto Rico y al Prof. Rafael Irizarry
por facilitar datos sobre pruebas moleculares en Puerto Rico.