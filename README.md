<p align="center">
  <img title="Redash" src='https://redash.io/assets/images/logo.png' width="200px"/>
</p>

## Deploy Manual

Actualizar la version en los files package.json y /redash/__init__.py . Actualizar el build en el archivo /client/app/version.json (fecha).

Actualizar la version al crear el tag de Docker.

```bash
export DOCKER_REGISTRY=251737917366.dkr.ecr.us-east-1.amazonaws.com

#build
pip3 install -r requirements_bundles.txt
npm install --global --force yarn@1.22.10
yarn bundle
docker build --build-arg skip_dev_deps=true -t 251737917366.dkr.ecr.us-east-1.amazonaws.com/redash:10.1.1-frubana .

#deploy
#docker login (en ec2 hay un paquete que logea automaticamente)
aws ecr get-login-password --region us-east-1 | sudo docker login --username AWS --password-stdin $DOCKER_REGISTRY
sudo docker push 251737917366.dkr.ecr.us-east-1.amazonaws.com/redash:10.1.1-frubana
```

## Test Local

```bash
export DOCKER_REGISTRY=251737917366.dkr.ecr.us-east-1.amazonaws.com

aws ecr get-login-password --profile=data.admin | docker login --username AWS --password-stdin $DOCKER_REGISTRY
docker pull 251737917366.dkr.ecr.us-east-1.amazonaws.com/redash:10.1.1-frubana

docker-compose up --force-recreate --build
docker-compose run --rm server create_db
docker-compose run --rm server manage db upgrade
```


COMO ACTUALIZAR:

Lanzar una nueva instancia desde el launch template que corresponda.

Ingresar a la instancia y ejecutar:

Modificar la version segun corresponda.
```bash
export DOCKER_REGISTRY=251737917366.dkr.ecr.us-east-1.amazonaws.com
aws ecr get-login-password --region us-east-1 | sudo docker login --username AWS --password-stdin $DOCKER_REGISTRY
sudo docker pull 251737917366.dkr.ecr.us-east-1.amazonaws.com/redash:10.1.1-frubana
cd /opt/redash
sudo docker-compose rm -s -f
#actualizar la version en el docker-compose.yml
sudo docker-compose up -d
sudo docker rmi 19b4394daf76
```

Crear una nueva AMI para la EC2 correspondiente, en el nombre ingresar la fecha.

Crear un nuevo Launch template con la version correspondiente de la AMI.

Lanzar un instance refresh del ASG.