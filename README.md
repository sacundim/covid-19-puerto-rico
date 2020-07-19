# Datos y análisis del COVID-19 en Puerto Rico

Este proyecto recopila datos sobre COVID-19 publicados por el 
Departamento de Salud de Puerto Rico en un formato legible por
computadora, y contiene algunos análisis de estos.

Visita el dashboard en:

* https://sacundim.github.io/covid-19-puerto-rico/


## Datos de fuente y archivos CSV

Los datos provienen principalmente de los [informes de Casos Positivos COVID-19](http://www.salud.gov.pr/Estadisticas-Registros-y-Publicaciones/Pages/COVID-19.aspx)
del Departamento de Salud de Puerto Rico, en algunos casos suplementados 
por fuentes misceláneas como reportes de prensa o informes y gráficas
de Salud que no aparecen (al momento) en ese enlace pero que se han
compartido con periodistas.

En el directorio [`assets/source_material/`](assets/source_material/)
se recopilan imágenes de boletines y gráficas, según este esquema:

* Archivos PDF originales de los informes en 
  [`assets/source_material/pdf/`](assets/source_material/pdf/);
* Archivos de imágenes extraídos de estos en directorios
  fechados dentro de [`assets/source_material/`](assets/source_material/).

Una selección de datos de estos se ha copiado a mano a los archivos CSV 
en el subdirectorio [`assets/data/`](assets/data/), que incluyen:

* [`PuertoRico-bulletin.csv`](assets/data/cases/PuertoRico-bulletin.csv), que
  consiste de números de "anuncio" que se hacen todos los días.
  Estas son las cifras que más publicidad reciben.
* [`PuertoRico-bitemporal.csv`](assets/data/cases/PuertoRico-bitemporal.csv),
  que consiste de datos de gráficas que acompañan estos boletines
  y que atribuyen las muertes a la fecha en que en verdad sucedieron
  y los casos positivos a la fecha que se tomó la muestra.
* [`PuertoRico-bioportal.csv`](assets/data/cases/PuertoRico-bioportal.csv),
  que consiste de datos del informe sobre pruebas entradas al bioportal
  (que al momento no están en PDF).


## Esquema bitemporal

Esta colección de datos se ha enfocado en presentar los datos de 
los gráficos en un llamado **esquema bitemporal**, donde se 
clasifica cada punto por **dos** fechas:

* La fecha de publicación de la cifra (columna `bulletin_date`);
* La fecha del evento que la cifra pretende describir (columna
  `datum_date`).

La intención de esto es posibilitar análisis de, por ejemplo:
 
* Cuándo es que de verdad se realizaron las pruebas o se murieron
  las personas vs. cuándo lo anunció Salud;
* Velar si hay problemas de calidad de datos, por ejemplo si 
  casos que aparecen en boletines más tempranos desaparecen
  de boletines más tardíos (¡que sí ocurre!).

Por ejemplo, esta gráfica de casos positivos probables hasta el 
2 de mayo del 2020 da 9 casos probables para el 1ero de abril:

![Casos probables hasta 2 de mayo](assets/source_material/2020-05-02/2020-05-02_probable.png)

Pero la gráfica del boletín del próximo día (datos hasta 3 de mayo 
del 2020) da 8 casos probablespara la misma fecha de 1ero de abril:

![Casos probables hasta 3 de mayo](assets/source_material/2020-05-03/2020-05-03_probable.jpeg)

Así que el archivo bitemporal reporta:

    bulletin_date,datum_date,confirmed_and_probable_cases,confirmed_cases,probable_cases,deaths
    2020-05-02,2020-04-01,80,71,9,3
    2020-05-03,2020-04-01,79,71,8,3

De nuevo, esta recopilación de datos intenta facilitar tales
observaciones.


## Base de datos PostgreSQL

Se incluye una configuración de Docker para lanzar una base de 
datos PostgreSQL con los datos extraídos de los boletines, y 
ciertos "views" para consultarlos más fácil y analizarlos.  Para
lanzarlo hay que tener Docker Compose y ejecutar desde este 
directorio:

    docker-compose up

La base de datos aparece en `localhost:5432`, usuario `postgres`,
contraseña `password`.

Para destruir la base de datos:

    docker-compose down

Para entender las vistas que se ofrecen de los datos, se puede
consultar el código que define el esquema o los metadatos de
este en la base de datos.  El código está aquí:

* [`postgres/010-schema.sql`](postgres/010-schema.sql)


## Análisis y gráficas

Hay además aquí código Python para generar una página web con una
serie de análisis y gráficas.  La forma más sencilla de lanzarlo 
requiere Docker y Docker Compose. Desde este directorio:

1. `docker-compose up` (inicia base de datos y servidor HTTP local;
    **ADVERTENCIA:** descarga como 300 MiB);
2. `./scripts/build-docker-image.sh` (**ADVERTENCIA:** Descarga más 
   de 1 gigabyte de datos);
3. `./scripts/run-in-docker.sh`;
   
...y navegar a [`http://localhost:8078/`](http://localhost:8078/):

* Casos cumulativos por fecha anuncio y fecha evento;
* Tiempo de duplicación de casos por ventanas de 7,
  14 y 21 días (por fecha de evento);
* Cambios de boletín a boletín ("deltas") para misma 
  fecha de evento;
* Estimado de rezago promedio en reporte de casos 
  (comparando boletines consecutivos).


## Agradecimientos

A Robby Cortés (@RobbyCortes en Twitter) y Angélica Serrano-Román
(@angelicaserran0) que diligentemente publican los boletines del
Departamento de Salud todas las mañanas.

A Danilo Pérez por muchas sugerencias valiosas.

Al Fideicomiso de Salud de Puerto Rico y al Prof. Rafael Irizarry
por facilitar datos sobre pruebas moleculares en Puerto Rico.