CREATE SCHEMA IF NOT EXISTS ctc_integration;

CREATE TABLE IF NOT EXISTS ctc_integration.received_verification_results_log (
    "clientCode" character varying(255) NOT NULL,
    "visitId" character varying(255) NOT NULL,
    "hfrCode" character varying(255) NOT NULL,
    "verificationDate" date NOT NULL,
    "hivFinalVerificationResultCode" character varying(64) NOT NULL,
    "ctcId" character varying(255),
    event_date timestamp with time zone NOT NULL,
    date_processed timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT received_verification_results_log_pk PRIMARY KEY ("clientCode", "visitId")
);
