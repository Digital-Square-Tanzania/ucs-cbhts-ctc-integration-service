# UCS-CBHTS-CTC INTEGRATION SERVICE

A service that exposes a production-ready integration endpoint for CTC to HTS data extraction from OpenSRP PostgreSQL tables.

## 1. Dev Requirements

 1. Java 17
 2. IntelliJ or Visual Studio Code
 3. Gradle

## 2. Run Locally

Set database environment variables (no hardcoded credentials):

```bash
export OPENSRP_DB_HOST=localhost
export OPENSRP_DB_PORT=5432
export OPENSRP_DB_NAME=opensrp
export OPENSRP_DB_SCHEMA=public
export OPENSRP_DB_USER=<db_user>
export OPENSRP_DB_PASSWORD=<db_password>
```

Then build and run:

```
  ./gradlew clean shadowJar
  java -jar build/libs/ucs-lab-module-integration-service-<version>.jar
```

## 3. Endpoint

- `POST /integration/ctc2hts`
- `GET /health`

Sample request body:

```json
{
  "hfrCode": "124899-6",
  "startDate": 1768262400,
  "endDate": 1768262800,
  "pageIndex": 1,
  "pageSize": 100
}
```

The response shape follows `resources/sample_output.json`.

Mapping references used by the service:
- `resources/CTC2HTSVariables_Integration_mappings.csv`
- `resources/reference_openrp_forms/`

## 4. Deployment via Docker

First Install docker in your PC by following [this guide](https://docs.docker.com/engine/install/). Secondly, clone this repo to your computer by using git clone and the repo's address:

`git clone https://github.com/Digital-Square-Tanzania/ucs-lab-integration-service.git`

Once you have completed cloning the repo, go inside the repo in your computer: `cd ucs-lab-integration-service`

Update `application.conf` found in `src/main/resources/` with the correct configs and use the following Docker commands for various uses:

### Run/start
`docker build -t ucs-lab-integration-service .`

`docker run -d -p 127.0.0.1:9202:9202 ucs-lab-integration-service`


### Interact With Shell

`docker exec -it ucs-lab-integration-service sh`

### Stop Services

`docker stop ucs-lab-integration-service`

## License

ISC

## Author

Ilakoze Jumanne

## Version
1.0.0
