FROM bitnami/spark:3.5.1

# Install the ping utility
USER root
RUN apt-get update && \
    apt-get install -y iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists