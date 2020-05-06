# Datos y análisis del COVID-19 en Puerto Rico

Este proyecto recopila datos sobre COVID-19 publicados por el 
Departamento de Salud de Puerto Rico en un formato legible por
computadora, y contiene algunos análisis de estos.


## Datos de fuente y archivos CSV

En el directorio [`source_material/`](source_material/) (al 
momento incompleto) se recopilan imágenes de boletines y
gráficas.  Una selección de datos de estos se ha copiado a 
mano a los archivos CSV en el subdirectorio [`data/`](data/),
que al momento consisten de:

* [`PuertoRico-bulletin.csv`](data/PuertoRico-bulletin.csv), que
  consiste de números de "anuncio" que se hacen todos los días.
  Estas son las cifras que más publicidad reciben.
* [`PuertoRico-bitemporal.csv`](data/PuertoRico-bitemporal.csv),
  que consiste de datos de gráficas que acompañan estos boletines
  y que atribuyen las muertes a la fecha en que en verdad sucedieron
  y los casos positivos a la fecha que se tomó la muestra.


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

![Casos probables hasta 2 de mayo](source_material/2020-05-02_probable.png)

Pero la gráfica del boletín del próximo día (datos hasta 3 de mayo 
del 2020) da 8 casos probablespara la misma fecha de 1ero de abril:

![Casos probables hasta 3 de mayo](source_material/2020-05-03_probable.jpeg)

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

Hay además aquí código Python para generar una serie de análisis
gráficas que aun no he documentado.  Para los doctos que quieran
jugar con esto:

* Hay que tener Python 3.7+, Docker y Docker Compose;
* Hay que tener la herramienta [`Poetry`](https://python-poetry.org/docs/)
  instalada;
* `poetry install` desde este directorio;
* `docker-compose up` desde este directorio;
* `./scripts/generate-reports.sh` desde este directorio;
* `docker-compose down`

...y las gráficas aparecen dentro del directorio [`output/`](output/):

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