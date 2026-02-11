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

```bash
  ./gradlew clean shadowJar
  java -jar build/libs/ucs-lab-module-integration-service-1.0.0.jar
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

Example request:

```bash
curl --request POST "http://127.0.0.1:8080/integration/ctc2hts" \
  --header "Content-Type: application/json" \
  --data '{
    "hfrCode": "124899-6",
    "startDate": 1768262400,
    "endDate": 1768262800,
    "pageIndex": 1,
    "pageSize": 100
  }'
```

## 4. Deployment via Docker

Build the image:

```bash
docker build -t ucs-cbhts-ctc-integration-service .
```

Run container:

```bash
docker run -d \
  --name ucs-cbhts-ctc-integration-service \
  -p 127.0.0.1:8080:8080 \
  -e OPENSRP_DB_HOST=host.docker.internal \
  -e OPENSRP_DB_PORT=5432 \
  -e OPENSRP_DB_NAME=opensrp \
  -e OPENSRP_DB_SCHEMA=public \
  -e OPENSRP_DB_USER=<db_user> \
  -e OPENSRP_DB_PASSWORD=<db_password> \
  ucs-cbhts-ctc-integration-service
```

Optional:
- Use `OPENSRP_DB_URL` instead of host/port/name/schema.
- Use `OPENSRP_DB_SSLMODE` if SSL is required.

View logs:

```bash
docker logs -f ucs-cbhts-ctc-integration-service
```

Stop and remove container:

```bash
docker rm -f ucs-cbhts-ctc-integration-service
```

## License

ISC

## Author

Ilakoze Jumanne

## Version
1.0.0
