-- Table: public.cbhts_tests

-- DROP TABLE IF EXISTS public.cbhts_tests;

CREATE TABLE IF NOT EXISTS public.cbhts_tests
(
    event_date text COLLATE pg_catalog."default",
    event_id text COLLATE pg_catalog."default" NOT NULL,
    form_submission_id text COLLATE pg_catalog."default",
    event_type text COLLATE pg_catalog."default",
    entity_type text COLLATE pg_catalog."default",
    visit_date text COLLATE pg_catalog."default",
    base_entity_id text COLLATE pg_catalog."default" NOT NULL,
    type_of_test_kit_used text COLLATE pg_catalog."default",
    test_kit_batch_number text COLLATE pg_catalog."default",
    test_kit_expire_date text COLLATE pg_catalog."default",
    test_result text COLLATE pg_catalog."default",
    syphilis_test_results text COLLATE pg_catalog."default",
    test_type text COLLATE pg_catalog."default",
    hts_visit_group text COLLATE pg_catalog."default",
    location_id text COLLATE pg_catalog."default",
    provider_id text COLLATE pg_catalog."default",
    team text COLLATE pg_catalog."default",
    team_id text COLLATE pg_catalog."default",
    date_created bigint,
    CONSTRAINT cbhts_tests_pkey PRIMARY KEY (event_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cbhts_tests
    OWNER to postgres;