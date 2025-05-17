#!/bin/bash

echo "Attempting to stop all running Docker containers on the system..."
RUNNING_CONTAINERS=$(docker ps -q)
if [ -n "$RUNNING_CONTAINERS" ]; then
    echo "Found running containers. Attempting to stop them..."
    docker stop $RUNNING_CONTAINERS
    echo "Finished attempting to stop running containers."
else
    echo "No running Docker containers found to stop."
fi

echo "Attempting to remove all Docker containers on the system (stopped and running)..."
ALL_CONTAINERS=$(docker ps -aq)
if [ -n "$ALL_CONTAINERS" ]; then
    echo "Found containers to remove. Attempting to remove them..."
    # Ensure containers are stopped before removing. The previous step attempts to stop them.
    # If some were not stopped, `docker rm` might fail for those without -f.
    # For a more forceful removal of any remaining running containers, one might use `docker rm -f $ALL_CONTAINERS`.
    # However, stopping first is generally safer.
    docker rm $ALL_CONTAINERS
    echo "Finished attempting to remove containers."
else
    echo "No Docker containers found to remove."
fi

echo "Additionally, running 'docker compose down' to clean up project-specific resources (networks, volumes)..."
# This command cleans up resources defined in the docker-compose.yaml for the current project.
# It will also attempt to stop and remove containers managed by compose if they were somehow missed by the commands above,
# though the primary purpose here is to clean up networks, volumes, and orphaned compose containers.
docker compose down -v --remove-orphans

echo "Process to stop/remove all Docker containers and clean up project resources is complete."