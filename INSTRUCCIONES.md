# Cómo reproducir todo esto

Estas son instrucciones un tanto laboriosas de cómo reproducir esta
página en el triste caso de que por ejemplo al autor se lo lleven
arrestado por informar demasiado bien al público.


## Requisitos de software

Esto solo aplica a quien quiera correr el software para generar la página,
no a quien solo le interese visitar la que genero yo:

1. Python 3.7 o posterior. Preferiblemente 3.7; se pueden manejar
   múltiples versiones a la vez con la herramienta [pyenv](https://github.com/pyenv/pyenv),
   on en una Mac con [Homebrew](https://brew.sh/) (`brew install python@3.7`).
2. [Poetry](https://python-poetry.org/) (manejador de dependencias Python)
3. Docker y Docker Compose
4. Entorno Unix con `bash`
5. [`wget`](https://www.gnu.org/software/wget/)
6. [`jq`](https://stedolan.github.io/jq/)
7. [AWS Command Line Interface](https://aws.amazon.com/cli/)

No todos los componentes de este proyecto requiren todas estas dependencias.

Una vez se tiene la versión adecuada de Python y la herramienta Poetry, hay que
crear un "virtualenv" (entorno aislado) con las demás dependencias de este software.
Entrar a este directorio y correr:

```python
# Modificar para apuntar al comando de python. Recomiendo 3.7
# porque cuando intenté 3.9 habían menos paquetes precompilados
# y era un pugilato lento.
poetry env use /path/to/python3.7  

# Instalar dependencias
poetry install

# Lanzar un "shell" con el virtualenv de python
poetry shell
```

## Bases de datos

### PostgreSQL

Se incluye una configuración de Docker para lanzar una base de 
datos PostgreSQL con los datos extraídos de los boletines, y 
ciertos "views" para consultarlos más fácil y analizarlos.  Para
lanzarlo solo hay que tener Docker Compose y ejecutar desde este 
directorio:

    docker-compose up

La base de datos aparece en `localhost:5432`, usuario `postgres`,
contraseña `password`.

Para destruir la base de datos:

    docker-compose down -v

Importante usar la opción `-v` aquí porque si no, Docker no destruye el volumen
de datos que corresponde a la base de datos, que ocupa varios gigaoctetos.  De
olvidársele esto se pueden borrar *con mucho cuidado* con `docker volume rm`
o **con mayor cuidado aún** con `docker volume prune`.

Para entender las vistas que se ofrecen de los datos, se puede
consultar el código que define el esquema o los metadatos de
este en la base de datos.  El código está aquí:

* [`postgres/010-schema.sql`](postgres/010-schema.sql)


### Amazon Athena

Los análisis y visualizaciones de datos de Bioportal se realizan en la nube de 
Amazon con [el servicio Athena](https://aws.amazon.com/athena/), que a esta fecha 
(diciembre del 2020) solo me está incurriendo en costos de $1 a $2 mensuales. 

Los "scripts" de SQL para montar el "data lake" y realizar las transformaciones son:

* [`create-sources-schema.sql`](aws/athena/create-sources-schema.sql), que
  define una base de datos en Athena que accede a los archivos Parquet de las
  descargas crudas de Bioportal.  Este solo hace falta correrlo cuando hay cambios
  mayores a la forma que se organizan estos.
* [`run-bioportal-etl.sql`](aws/athena/run-bioportal-etl.sql), que procesa los 
  datos crudos para crear una base de datos más refinada.  Este suele correrse diario.

Reproducir este "data lake" requeriría:
 
1. Montar un entorno en AWS ([descrito de forma bien incompleta aquí](aws/README.md)).
2. Instalar los prerequisitos de los "shell scripts" de descargas de Bioportal.
3. Modificar los "shell scripts" y los de SQL para quitar las referencias a mano 
   ("hardcoded") al entorno AWS de este autor.
4. Realizar descargas diarias de Bioportal, convertirlas a Parquet y subirlas a S3, 
   todo automatizado por los "shell scripts" aquí provistos. 
5. Correr los archivos SQL en Athena.

Los "shell scripts" requieren:

* Instalación de las herramientas `poetry`, `wget` y `jq`;
* Correr un `poetry install` desde este directorio, que instala (entre muchas otras 
  cosas) una herramienta llamada `csv2parquet`;
* Activación del `virtualenv` Python que define este proyecto; este se puede
  activar corriendo el comando `poetry shell` en este directorio.

Las descargas diarias y conversión de estas a formato Parquet están automatizadas 
en este "shell script":

* [`scripts/bioportal-download.sh`](scripts/bioportal-download.sh)

Este "script" coloca los archivos descargados bajo una jerarquía dentro del
directorio [`s3-bucket-sync/covid-19-puerto-rico-data/`](s3-bucket-sync/covid-19-puerto-rico-data/),
Al momento (diciembre 2020) cada descarga produce un tanto más de 200 MB de
datos, así que este directorio puede crecer bastante.

La subida de las descargas en aquel directorio al almacenaje S3 en Amazon está 
automatizada en este "shell script":

* [`scripts/aws-data-sync.sh`](scripts/aws-data-sync.sh)

Este "script" solo sube los archivos que no estén ya presentes en la nube.
Después de subir los archivos es seguro borrar las copias locales, que no se 
usan para nada.

Todos estos "scripts" y los del SQL en [`aws/athena`](aws/athena) tienen escrito 
a mano los nombres de los "buckets" de S3 que haría falta cambiarlos para reproducir
estas páginas.  

Al momento con una sola descarga de Bioportal es posible generar todo el contenido
de la página, pero eso no siempre ha sido cierto y pudiera en el futuro volver
a no serlo.  Si alguien tiene interés en acceder a mi colección de descargas diarias
(que se remontan hasta julio del 2020) podrían hacerse arreglos para compartir esta.
Al momento (diciembre del 2020) son más de 15 GB de datos.


## Análisis y gráficas

La forma más sencilla de lanzar la aplicación que genera las páginas 
es con Docker y Docker Compose. Desde este directorio:

1. `docker-compose up` (inicia base de datos PostgreSQL y servidor 
   HTTP local; **ADVERTENCIA:** descarga como 300 MiB la primera vez);
2. `./scripts/build-docker-image.sh` (**ADVERTENCIA:** Descarga más 
   de 1 gigabyte de datos la primera vez y cada pocas semanas);
3. `./scripts/run-in-docker.sh`

Esto coloca el contenido web (todo estático) en el directorio `output/`.
El entorno de Docker Compose contiene un servidor web apuntando a ese
directorio, que se puede ver visitando [`http://localhost:8078/`](http://localhost:8078/).

Cuando ya no hace falta la base de datos PostgreSQL ni el servidor web
local, `docker-compose down -v` desde este directorio.