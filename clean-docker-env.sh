#!/bin/bash
# Stop and remove all containers
docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q)

# Remove all volumes
docker volume rm $(docker volume ls -q)

# Remove all images
docker rmi $(docker images -a -q)

# Remove all networks (optional)
docker network rm $(docker network ls -q)
