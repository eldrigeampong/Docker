# Containerized Python Scripts With Docker
These projects build, run docker images as a container and deploy images using Docker Hub. 

## Installation
```
py -m pip freeze > requirements.txt
py -m pip install -r requirements.txt

```

## Executable
```
py -m main.py

```

## Copy files/folders between a container and the local filesystem
```
docker container cp [OPTIONS] CONTAINER:SRC_PATH DEST_PATH|-
docker cp [OPTIONS] SRC_PATH|- CONTAINER:DEST_PATH

```

![My Image](https://www.ianlewis.org/assets/images/docker/large_v-trans.png)
![My Image](https://jfrog.file.force.com/servlet/servlet.ImageServer?id=01569000008kqFT&oid=00D20000000M3v0&lastMod=1631619825000)