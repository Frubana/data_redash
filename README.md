<p align="center">
  <img title="Redash" src='https://redash.io/assets/images/logo.png' width="200px"/>
</p>


## Nueva versión:

Una vez realizados los cambios se tiene que actualizar el archivo VERSION con la nueva version,
una vez que los cambios se mergeen en `master-frubana` se ejecuta un pipeline que crea la nueva imagen en ECR.


## LOCAL DEVELOPMENT.

Se tiene que utilizar Node 12 y python 3.7

Se puede utilizar nvm para tener distintas versiones de node instalada en el SO.

```sh
nvm install 12
nvm use 12
```

Crear un virtual env con python 3.7, se recomienda utilizar un nuevo interprete en PyCharm cuando se
levante el proyecto.

### FRONTEND:

Para levantar el frontend en modo desarrollo se tiene que modificar en el archivo package.json y package-lock.json,
la propiedad "version", quitar el texto __version__ y remplazarlo por algun numero, este cambio nunca debe ser subido
ya que el pipeline utiliza ese placeholder para crear una nueva release.

```sh
pip3 install -r requirements_bundles.txt
npm ci
npm start
```

## BACKEND:
Para levantar la api en modo desarrollo se levanta con contenedor con docker,
el servidor monta el volumen donde se encuentran los archivos del backend.

```shell
docker-compose up server
docker-compose run --rm server create_db
docker-compose run --rm server manage db upgrade
```

### Instalar librerias python en el virtualenv

Ya que existen muchos conflictos de versiones se tiene que utilizar una version especifica de pip
para que las resuelva sin problemas.

```shell
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_CACHE_DIR=1

pip install pip==20.2.4;
pip install -r requirements.txt
```


## Comandos Utiles

### Crear base de datos
Esto es para inicializar una base de datos vacia.
```sh
make create_database
```

### Actualizar base de datos
Esto es para inicializar una base de datos vacia.
```sh
make apply-migration
```

### Crear una nueva migracion
Si se crea un nuevo modelo para crear la migracion correspondiente:
https://flask-migrate.readthedocs.io/en/latest/

```sh
make create-migration
```

### Tests

Para que todos los tests se ejecuten correctamente se tienen que crear los archivos _./client/dist/index.html_
y _./client/dist/multi_org.html_ (con que esten vacios funciona) o ejecutar npm build.

**Nota**: Los únicos tests que no funcionan son los del archivo test_athena.py .

Este comando crea la base de datos para Test y los ejecuta.
```sh
make backend-unit-tests
```

Para solamente ejecutar los tests (la base de datos ya existe)
```sh
make tests
```


