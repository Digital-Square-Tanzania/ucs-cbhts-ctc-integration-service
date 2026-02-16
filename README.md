# UCS-CBHTS-CTC Integration Service

This service exposes integration APIs used to:

1. Extract and transform CBHTS/CTC OpenSRP data into the HTS integration payload shape.
2. Receive HIV verification outcomes and forward them to OpenSRP as events.
3. Receive CTC LTF/MISSAP and index-contact payloads and forward mapped clients/events/tasks to OpenSRP.

The service is built with Java 17, Akka HTTP, and Gradle.

## Table of Contents

1. [Architecture](#architecture)
2. [API Reference](#api-reference)
3. [Configuration](#configuration)
4. [Build, Test, and Run](#build-test-and-run)
5. [Docker Deployment](#docker-deployment)
6. [Data Dependencies](#data-dependencies)
7. [Mapping and Transformation Assets](#mapping-and-transformation-assets)
8. [Error Handling and Troubleshooting](#error-handling-and-troubleshooting)

## Architecture

### High-Level Flow

```text
Client
  -> Akka HTTP Routes
    -> Request Validation
      -> Service Layer
        -> PostgreSQL Repository (read)
        -> Data Mapper (shape + normalize + optional encryption)
        -> OpenSRP Event/Task Sender
  <- JSON Response
```

### Main Components

- `src/main/java/com/abt/UcsCbhtsCtsIntegrationServiceApp.java`
  - Application entrypoint.
  - Starts Akka HTTP server using `INTEGRATION_SERVICE_HOST` and `INTEGRATION_SERVICE_PORT` (loaded from env or `.env`).

- `src/main/java/com/abt/UcsCbhtsCtsIntegrationRoutes.java`
  - Exposes:
    - `GET /health`
    - `POST /integration/ctc2hts`
    - `POST /integration/verification-results`
  - Converts validation failures to `400` and unexpected failures to `500`.

- `src/main/java/com/abt/UcsCtcIntegrationRoutes.java`
  - Exposes:
    - `POST /send-ltf-missap-clients`
    - `POST /send-index-contacts`
  - Uses actor-based processing and returns `400` when downstream response description contains `error`.

- `src/main/java/com/abt/integration/service/OpenSrpIntegrationService.java`
  - Handles `/integration/ctc2hts` requests.
  - Orchestrates validation, DB reads, mapping, pagination metadata.

- `src/main/java/com/abt/integration/service/OpenSrpVerificationResultsService.java`
  - Handles `/integration/verification-results`.
  - Validates input, resolves latest service metadata by client/hfr, builds OpenSRP events, forwards events.

- `src/main/java/com/abt/integration/db/OpenSrpIntegrationRepository.java`
  - Contains all SQL queries and DB row records.
  - Reads from CBHTS-related OpenSRP tables and groups related records.

- `src/main/java/com/abt/integration/mapping/IntegrationDataMapper.java`
  - Maps DB rows to final payload sections.
  - Applies reference mappings and aliases.
  - Conditionally encrypts identity/name fields based on config.

- `src/main/java/com/abt/integration/mapping/MappingReferenceCatalog.java`
  - Loads integration code mappings from CSV and OpenSRP form JSON options.

## API Reference

### 1) Health Check

`GET /health`

Response:

```json
{
  "status": "ok"
}
```

### 2) CTC to HTS Integration

`POST /integration/ctc2hts`

Request body:

```json
{
  "hfrCode": "124899-6",
  "startDate": 1768262400,
  "endDate": 1768262800,
  "pageIndex": 1,
  "pageSize": 100
}
```

Validation rules:

- `hfrCode` is required.
- `startDate` is required.
- `endDate` is required.
- `startDate <= endDate`.
- `pageIndex >= 1`.
- `pageSize >= 1`.

Notes:

- Request timestamps are expected in epoch seconds.
- Query logic also checks millisecond-stored records by comparing against `startDate * 1000` and `endDate * 1000`.

Success response shape:

```json
{
  "pageNumber": 1,
  "pageSize": 100,
  "totalRecords": 123,
  "data": [
    {
      "htcApproach": "CBHTS",
      "visitDate": "2026-01-14",
      "counsellor": {},
      "clientCode": "CLT123456",
      "cellPhoneNumber": "2557...",
      "clientIdentification": {},
      "clientName": {},
      "demographics": {},
      "residence": {},
      "clientClassification": {},
      "testingHistory": {},
      "currentTesting": {},
      "selfTesting": [],
      "reagentTesting": [],
      "hivResultCode": "POSITIVE",
      "preventionServices": {},
      "referralAndOutcome": [],
      "remarks": "Generated from cbhts_services event ...",
      "createdAt": 1768262800
    }
  ]
}
```

Full output example: `resources/sample_output.json`

### 3) Verification Results Forwarding

`POST /integration/verification-results`

Request body:

```json
{
  "hfrCode": "12123-1",
  "data": [
    {
      "clientCode": "CLT123456",
      "verificationDate": "2026-01-01",
      "hivFinalVerificationResultCode": "POSITIVE",
      "ctcId": "12-11-2132-133214",
      "visitId": "B0452823-F078-4CAC-8746-4A11733E942A"
    }
  ]
}
```

Validation rules:

- `hfrCode` is required.
- `data` must exist and be non-empty.
- For each `data[i]`:
  - `clientCode` is required.
  - `verificationDate` is required and must be `yyyy-MM-dd`.
  - `hivFinalVerificationResultCode` is required and must be one of:
    - `POSITIVE`
    - `NEGATIVE`
    - `INCONCLUSIVE`
  - `visitId` is required.
- `ctcId` is optional.

Success response:

```json
{
  "processedCount": 1,
  "successCount": 1,
  "skippedCount": 0,
  "failureCount": 0,
  "errors": []
}
```

Partial failures are returned in the `errors` array with item index and message.
Duplicate verification results (same `clientCode` + `visitId`) are skipped.

### 4) Send LTF/MISSAP Clients

`POST /send-ltf-missap-clients`

Representative request body:

```json
{
  "team_id": "team-id",
  "team": "Team A",
  "location_id": "location-id",
  "provider_id": "provider-id",
  "rec_guid": "REC-GUID",
  "ctc_number": "12345678",
  "hfr_code": "124899-6",
  "last_appointment_date": "2026-01-01",
  "client_first_name": "encrypted-first-name",
  "client_middle_name": "encrypted-middle-name",
  "client_last_name": "encrypted-last-name",
  "client_phone_number": "encrypted-phone",
  "client_sex": "M",
  "client_dob": "1990-01-01"
}
```

Notes:

- Encrypted input fields in this payload are decrypted using the LTF/Index payload encryption key (`LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY`).

Success response (`200`):

```json
{
  "response": {
    "description": "sending successful",
    "unique_id": "generated-opensrp-id",
    "base_entity_id": "generated-base-entity-id"
  }
}
```

### 5) Send Index Contacts

`POST /send-index-contacts`

Representative request body:

```json
{
  "team_id": "team-id",
  "team": "Team A",
  "location_id": "location-id",
  "provider_id": "provider-id",
  "rec_guid": "REC-GUID",
  "ctc_unique_id": "ctc-uid",
  "elicitation_date": "2026-01-01",
  "first_name": "encrypted-first-name",
  "middle_name": "encrypted-middle-name",
  "last_name": "encrypted-last-name",
  "sex": "F",
  "dob": "1994-02-01",
  "primary_phone_number": "encrypted-primary-phone",
  "alternative_phone_number": "encrypted-alt-phone",
  "notification_method": "CR",
  "relationship": "SP",
  "ctc_number": "12345678"
}
```

Notes:

- Encrypted input fields in this payload are decrypted using the LTF/Index payload encryption key (`LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY`).

Success response (`200`) has the same shape as `/send-ltf-missap-clients`.

### Error Responses

Validation errors return HTTP `400`:

```json
{
  "message": "Invalid request payload",
  "details": [
    "data[0].visitId is required"
  ]
}
```

Unhandled processing failures return HTTP `500`:

```json
{
  "message": "Failed to process integration request",
  "details": [
    "..."
  ]
}
```

### Example cURL Commands

CTC to HTS extraction:

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

Verification forwarding:

```bash
curl --request POST "http://127.0.0.1:8080/integration/verification-results" \
  --header "Content-Type: application/json" \
  --data '{
    "hfrCode": "12123-1",
    "data": [
      {
        "clientCode": "CLT123456",
        "verificationDate": "2026-01-01",
        "hivFinalVerificationResultCode": "POSITIVE",
        "ctcId": "12-11-2132-133214",
        "visitId": "B0452823-F078-4CAC-8746-4A11733E942A"
      }
    ]
  }'
```

LTF/MISSAP forwarding:

```bash
curl --request POST "http://127.0.0.1:8080/send-ltf-missap-clients" \
  --header "Content-Type: application/json" \
  --data '{
    "team_id": "team-id",
    "team": "Team A",
    "location_id": "location-id",
    "provider_id": "provider-id",
    "rec_guid": "REC-GUID",
    "ctc_number": "12345678",
    "hfr_code": "124899-6",
    "last_appointment_date": "2026-01-01",
    "client_first_name": "encrypted-first-name",
    "client_middle_name": "encrypted-middle-name",
    "client_last_name": "encrypted-last-name",
    "client_phone_number": "encrypted-phone",
    "client_sex": "M",
    "client_dob": "1990-01-01"
  }'
```

Index contacts forwarding:

```bash
curl --request POST "http://127.0.0.1:8080/send-index-contacts" \
  --header "Content-Type: application/json" \
  --data '{
    "team_id": "team-id",
    "team": "Team A",
    "location_id": "location-id",
    "provider_id": "provider-id",
    "rec_guid": "REC-GUID",
    "ctc_unique_id": "ctc-uid",
    "elicitation_date": "2026-01-01",
    "first_name": "encrypted-first-name",
    "middle_name": "encrypted-middle-name",
    "last_name": "encrypted-last-name",
    "sex": "F",
    "dob": "1994-02-01",
    "primary_phone_number": "encrypted-primary-phone",
    "alternative_phone_number": "encrypted-alt-phone",
    "notification_method": "CR",
    "relationship": "SP",
    "ctc_number": "12345678"
  }'
```

## Configuration

Copy `.env-sample` and adjust values:

```bash
cp .env-sample .env
set -a
source .env
set +a
```

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENSRP_DB_URL` | No | None | Full JDBC URL. If set, host/port/name/schema values are not used to build URL. |
| `OPENSRP_DB_HOST` | No | `localhost` | PostgreSQL host (used when `OPENSRP_DB_URL` is not set). |
| `OPENSRP_DB_PORT` | No | `5432` | PostgreSQL port. |
| `OPENSRP_DB_NAME` | No | `opensrp` | PostgreSQL database name. |
| `OPENSRP_DB_SCHEMA` | No | `public` | PostgreSQL schema used in SQL queries. Must match `^[A-Za-z0-9_]+$`. |
| `OPENSRP_DB_USER` | Usually | None | PostgreSQL username. |
| `OPENSRP_DB_PASSWORD` | Usually | None | PostgreSQL password. |
| `OPENSRP_DB_SSLMODE` | No | None | Optional SSL mode (`disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`). |
| `OPENSRP_SERVER_EVENT_URL` | Yes for verification and `/send-*` endpoints | None | OpenSRP event endpoint URL (for example `http://host:8080/opensrp/rest/event/add`). `/send-*` endpoints derive the OpenSRP base URL from this value. |
| `OPENSRP_SERVER_URL` | Fallback | None | Fallback OpenSRP base URL used when `OPENSRP_SERVER_EVENT_URL` is unset. |
| `OPENSRP_SERVER_USERNAME` | No | None | Basic auth username used by verification and `/send-*` forwarding. |
| `OPENSRP_SERVER_PASSWORD` | No | None | Basic auth password used by verification and `/send-*` forwarding. |
| `LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY` | Yes for `/send-ltf-missap-clients` and `/send-index-contacts` | `secret-key` | LTF/Index payload encryption key (decrypt/encrypt flow key) used by `/send-ltf-missap-clients` and `/send-index-contacts`. |
| `INTEGRATION_SERVICE_HOST` | No | `127.0.0.1` | HTTP bind host for this service. |
| `INTEGRATION_SERVICE_PORT` | No | `8080` | HTTP bind port for this service. |
| `INTEGRATION_SERVICE_ROUTES_ASK_TIMEOUT` | No | `60s` | Ask timeout used by actor-backed `/send-*` endpoints. |
| `ENCRYPT_DATA` | No | `false` behavior | CBHTS payload encryption toggle for selected `/integration/ctc2hts` client identity fields; encryption is enabled only when value is exactly `true` (case-insensitive). |
| `CBHTS_PAYLOAD_ENCRYPTION_SECRET_KEY` | Conditionally | None | CBHTS payload encryption key used only when `ENCRYPT_DATA=true` (encrypts selected outbound `/integration/ctc2hts` identity fields). |

### Encryption Behavior

Two payload encryption keys are used for different flows:

- `LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY`: LTF/Index payload encryption key (decrypt/encrypt flow key) for `/send-ltf-missap-clients` and `/send-index-contacts`.
- `CBHTS_PAYLOAD_ENCRYPTION_SECRET_KEY`: CBHTS payload encryption key used only when `ENCRYPT_DATA=true` (encrypts selected outbound `/integration/ctc2hts` identity fields).

When `ENCRYPT_DATA=true`, the `/integration/ctc2hts` mapper encrypts only these outbound fields:

- `clientUniqueIdentifierType`
- `clientUniqueIdentifierCode`
- `firstName`
- `middleName`
- `lastName`

Implementation details:

- Encryption uses `Utils.encryptDataNew(...)` (AES/CBC/PKCS5Padding, Base64 output with IV prepended).
- If encryption is enabled and `CBHTS_PAYLOAD_ENCRYPTION_SECRET_KEY` is missing/blank, the service fails fast at startup.
- `null`, empty, and whitespace-only values are preserved as-is (not transformed).

## Build, Test, and Run

### Prerequisites

- Java 17
- Gradle wrapper (`./gradlew`) or compatible Gradle runtime

### Run tests

```bash
./gradlew test
```

### Build executable jar

```bash
./gradlew clean shadowJar
```

Generated artifact:

`build/libs/ucs-cbhts-ctc-integration-service-1.0.0.jar`

### Run service

```bash
java -jar build/libs/ucs-cbhts-ctc-integration-service-1.0.0.jar
```

Default bind address:

- Host: `127.0.0.1`
- Port: `8080`

Configured via `INTEGRATION_SERVICE_HOST` and `INTEGRATION_SERVICE_PORT` (see `.env-sample`).

## Docker Deployment

### Build image

```bash
docker build -t ucs-cbhts-ctc-integration-service .
```

### Run with explicit env values

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
  -e OPENSRP_SERVER_EVENT_URL=http://host.docker.internal:8080/opensrp/rest/event/add \
  -e OPENSRP_SERVER_USERNAME=<opensrp_user> \
  -e OPENSRP_SERVER_PASSWORD=<opensrp_password> \
  -e LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY=<secret_key> \
  ucs-cbhts-ctc-integration-service
```

### Run with env file

```bash
docker run -d \
  --name ucs-cbhts-ctc-integration-service \
  -p 127.0.0.1:8080:8080 \
  --env-file .env \
  ucs-cbhts-ctc-integration-service
```

### Run with env file (Linux)

On Linux, add `--add-host=host.docker.internal:host-gateway` so `host.docker.internal` resolves from inside the container.

```bash
docker run -d \
  --name ucs-cbhts-ctc-integration-service \
  -p 127.0.0.1:9600:8080 \
  --env-file .env \
  --add-host=host.docker.internal:host-gateway \
  ucs-cbhts-ctc-integration-service
```

### Logs and lifecycle

```bash
docker logs -f ucs-cbhts-ctc-integration-service
docker rm -f ucs-cbhts-ctc-integration-service
```

## Data Dependencies

This service reads OpenSRP PostgreSQL data from the configured schema.

Primary tables used:

- `cbhts_services`
- `cbhts_tests`
- `cbhts_enrollment`
- `client`
- `team_members`
- `tanzania_locations`
- `household`
- `hivst_results`
- `hivst_issue_kits`
- `ctc_integration.received_verification_results_log`

Reference SQL structures are available in:

- `resources/tables structures/`

## Mapping and Transformation Assets

Mapping references used by `MappingReferenceCatalog`:

- `resources/CTC2HTSVariables_Integration_mappings.csv`
- `resources/reference_openrp_forms/*.json`

These files drive code normalization and value translation for several output sections.

## Error Handling and Troubleshooting

### Common issues

- Database connection failure:
  - Verify DB host/port/credentials/schema and network reachability.
  - Confirm `OPENSRP_DB_SCHEMA` value is valid (alphanumeric + underscore only).

- Empty extraction response:
  - Confirm `hfrCode` exists in `tanzania_locations`.
  - Confirm date range and unit (seconds in request) align with stored event timestamps.

- Verification forwarding failures:
  - Confirm `OPENSRP_SERVER_EVENT_URL`/`OPENSRP_SERVER_URL`.
  - Verify auth credentials if destination requires Basic auth.
  - Check response `errors` array for per-item failure details.

- Startup failure after enabling encryption:
  - Ensure `CBHTS_PAYLOAD_ENCRYPTION_SECRET_KEY` is set and non-blank when `ENCRYPT_DATA=true`.

### Logging

- Logback config: `src/main/resources/logback.xml`
- Default root log level: `INFO`
- Logs are written to stdout.

## Additional Notes

- JSON payloads ignore unknown properties during deserialization.
- Pagination is request-driven via `pageIndex` and `pageSize`.
- CTC->HTS output uses deterministic section keys, while internal mapping logic normalizes source variants and aliases.
