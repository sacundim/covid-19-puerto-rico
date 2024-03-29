# Datos y análisis del COVID-19 en Puerto Rico

[**(For English click here)**](README.en.md)

Este projecto, en curso desde mayo del 2020, captura datos sobre el
COVID-19 en Puerto Rico y produce los análisis y visualizaciones que
se presentan en la siguiente web:

* https://covid-19-puerto-rico.org/

Entre los análisis que realiza este proyecto, resalto aquí los
siguientes:

* La curva de casos que se presenta en la página no es simplemente
  copiada de las cifras oficiales de casos del Departamento de Salud
  de Puerto Rico, sino que la recalculamos a partir de los datos
  crudos de resultados de pruebas en el sistema de Bioestadísticas.
  Históricamente (antes de mayo del 2023) este recálculo ha producido
  resultados más sensatos que los oficiales respecto por ejemplo a
  recontagios.  A principios de mayo del 2023 el Departamento de Salud
  revisó retroactivamente su curva de casos de tal forma que hoy en
  día cuadra con la metodología que uso desde el 2021.
* Análisis del rezago de los datos de pruebas y casos—quiérese decir,
  cuánto tiempo transcurre entre las fechas en que los datos recogen
  que se le tomó muestras a los pacientes, y cuándo aparece el récord
  de resultado de prueba o de conteo de caso en los sistemas de datos.
* Análisis de volumen, positividad y rezagos por prueba molecular
  vs. antígenos, que muestra interesantísimos patrones tales como:
  gradual crecida en la propoción de antígenos; tasa de positividad
  sistemáticamente menor para pruebas de antígenos; rezago mucho menor
  entre toma de muestra y recepción de datos de resultado para las
  pruebas de antígenos.


## Captura de datos

El subdirectorio [`downloader/`](downloader/) contiene la aplicación
de captura e ingesta de datos, que diariamente captura una selección
de datos de estas fuentes principales y los ingesta a un "lago de
datos" en Amazon S3:

1. Descargas CSV diarias del
   [tablero de estadísticas de COVID-19 del Departamento de Salud de Puerto Rico](https://www.salud.pr.gov/estadisticas_v2);
2. Descargas JSON diarias del
   [API de Bioestadísticas del Departamento de Salud de Puerto Rico](https://biostatistics.salud.pr.gov).
3. Descargas CSV a varios ritmos de
   [la página `HealthData.gov` del Departamento de Salud y Servicios Humanos de los Estados Unidos](https://healthdata.gov/)
   y
   [la semejante página `data.cdc.gov` de los Centros de Control de Enfermedades (CDC)](https://data.cdc.gov/);

Adicionalmente existe una colección de datos más viejos (muchos
capturados a mano) de estas fuentes:

* Los
  [informes de Casos Positivos de COVID-19](http://www.salud.pr.gov/Estadisticas-Registros-y-Publicaciones/Pages/COVID-19.aspx)
  del Departamento de Salud de Puerto Rico, publicados 2020 al 2021,
  luego remplazados por una función de descaga de archivos CSV.  Estos
  solo fueron publicados en formato PDF; este repositorio recoge todos
  los PDFs bajo
  [`website/assets/source_material/pdf/`](website/assets/source_material/pdf/),
  y archivos CSV dolorosamente copiados y verificados a mano de estos
  bajo [`website/assets/data/`](website/assets/data/).
* El API del Bioportal del Departamento de Salud de Puerto Rico, el
  predecesor del arriba mencionado de Bioestadísticas que tiene la
  misma información.
* Datos de hospitalizaciones del
  [COVID Tracking Project](https://covidtracking.com/), que a su vez
  los recopiló del Departamento de Salud de Puerto Rico y del
  Departamento de Salud y Servicios Humanos de los Estados Unidos.
* Fuentes misceláneas como reportes de prensa o informes y gráficas
  del Departamento de Salud de Puerto Rico que no aparecen en esos
  enlaces pero que se han compartido con periodistas.


## Limpieza y análisis de datos

El grueso de la limpieza y análisis de datos está construida en SQL
como un proyecto de [la herramienta DBT](https://www.getdbt.com/), y
corre bajo [Amazon Athena](https://aws.amazon.com/athena/), un
servicio de SQL en la nube. El código está en este directorio:

* [`athena-dbt/`](athena-dbt/)


## Generador de páginas estáticas

Las páginas web son 100% estático (no hay ningún código que ejecute en
servidores HTTP, solo HTML y Javascript en el cliente).  La aplicación
que las genera se halla en este directorio:

* [`website/`](website/)

Las gráficas son hechas con el excelente sistema
[Vega-Lite](https://vega.github.io/vega-lite/) y su interfaz en Python
[Vega-Altair](https://altair-viz.github.io/).


## Análisis Nextstrain (vigilancia genómica viral)

Aparte del proyecto en este repositorio, también mantengo
[otro repositorio](https://github.com/sacundim/covid-19-puerto-rico-nextstrain)
en que adapto y especializo a Puerto Rico el proyecto de filogenia
genómica viral de COVID-19 que elabora
[el proyecto Nextstrain](https://nextstrain.org/).  Mi visualización
[se puede ver en este enlace](https://nextstrain.org/fetch/covid-19-puerto-rico.org/auspice/ncov_puerto-rico.json?f_division=Puerto%20Rico).


## Agradecimientos

A Robby Cortés (@RobbyCortes en Twitter) y Angélica Serrano-Román
(@angelicaserran0) que diligentemente publicaron los boletines del
Departamento de Salud todas las mañanas en los dias tempranos de la
pandemia.

A Danilo Pérez Prof. Rafael Irizarry por muchas sugerencias
e información valiosa.

Al Fideicomiso de Salud de Puerto Rico por facilitar la publicación de
datos gubernamentales valiosos temprano en la pandemia.
