def get_static_services_block():
    return """version: "3.8"

services:
  # de-identification-ui:
  #   container_name: portal_ui
  #   image: deidentification_portal_ui:${DE_IDENTIFICATION_TAG_UI}
  #   ports:
  #     - "${DE_IDENTIFICATION_API_PORT_UI}:3000"
  #   restart: always
  
  de-identification-api:
    container_name: api
    image: deidentification_portal:2.0.0
    ports:
      - "${DE_IDENTIFICATION_API_PORT}:8000"
    env_file:
      - ${DE_IDENTIFICATION_ENV_FILE}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    command: bash /nd_deployment/run.sh
    restart: always
    volumes:
      - ${SETUP_CONFIG_PATH}:/nd_deployment/setup_config.json
    depends_on:
      de-identification-db:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8000/auth/login || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 3

  de-identification-notebook:
    container_name: notebook
    image: deidentification_portal:2.0.0
    ports:
      - "9888:8888"
    env_file:
      - ${DE_IDENTIFICATION_ENV_FILE}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    working_dir: /NOTEBOOK
    command: jupyter lab --ip=0.0.0.0 --allow-root --no-browser
    volumes:
      - de_identification_notebook:/NOTEBOOK
    restart: always

  de-identification-db:
    image: postgres:15
    container_name: deidentification_db
    environment:
      - POSTGRES_DB=${DE_IDENTIFICATION_DB_NAME}
      - POSTGRES_USER=${DE_IDENTIFICATION_DB_USER}
      - POSTGRES_PASSWORD=${DE_IDENTIFICATION_DB_PASSWORD}
    ports:
      - "6545:5432"
    volumes:
      - de_identification_portal_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
"""

def generate_worker_block(index):
    return f"""
  de-identification-worker-{index}:
    container_name: worker-{index}
    image: deidentification_portal:2.0.0
    env_file:
      - ${{DE_IDENTIFICATION_ENV_FILE}}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    command: python3.10 ./deIdentification/manage.py start_worker
    restart: always
    depends_on:
      de-identification-db:
        condition: service_healthy
      de-identification-api:
        condition: service_healthy"""

def generate_compose_with_workers(n):
    static_block = get_static_services_block()
    workers = [generate_worker_block(i+1) for i in range(n)]
    workers_block = "\n".join(workers)
    volumes_block = """
volumes:
  de_identification_portal_data:
    external: true
  de_identification_notebook:
    external: true
"""
    return static_block + workers_block + volumes_block

if __name__ == "__main__":
    n = 20  # Change to however many workers you want
    docker_compose_content = generate_compose_with_workers(n)
    with open("workers.yaml", "w") as f:
        f.write(docker_compose_content)
    print(f"Docker Compose file with {n} workers written to workers.yaml")
