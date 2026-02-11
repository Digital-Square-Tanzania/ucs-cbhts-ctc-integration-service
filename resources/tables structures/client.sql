-- Table: public.client

-- DROP TABLE IF EXISTS public.client;

CREATE TABLE IF NOT EXISTS public.client
(
    base_entity_id character varying COLLATE pg_catalog."default" NOT NULL,
    client_id character varying COLLATE pg_catalog."default" NOT NULL,
    unique_id character varying COLLATE pg_catalog."default" NOT NULL,
    first_name character varying COLLATE pg_catalog."default",
    middle_name character varying COLLATE pg_catalog."default",
    last_name character varying COLLATE pg_catalog."default",
    sex character varying COLLATE pg_catalog."default",
    age bigint,
    preg_1yr character varying COLLATE pg_catalog."default",
    disabilities character varying COLLATE pg_catalog."default",
    type_of_disability character varying COLLATE pg_catalog."default",
    national_id character varying COLLATE pg_catalog."default",
    voter_id character varying COLLATE pg_catalog."default",
    driver_license character varying COLLATE pg_catalog."default",
    passport character varying COLLATE pg_catalog."default",
    insurance_provider character varying COLLATE pg_catalog."default",
    insurance_provider_other character varying COLLATE pg_catalog."default",
    insurance_provider_number character varying COLLATE pg_catalog."default",
    marital_status character varying COLLATE pg_catalog."default",
    phone_number character varying COLLATE pg_catalog."default",
    occupation character varying COLLATE pg_catalog."default",
    occupation_other character varying COLLATE pg_catalog."default",
    reasons_for_registration character varying COLLATE pg_catalog."default",
    team character varying COLLATE pg_catalog."default",
    team_id character varying COLLATE pg_catalog."default",
    location_id character varying COLLATE pg_catalog."default",
    provider_id character varying COLLATE pg_catalog."default",
    event_date timestamp without time zone,
    family character varying COLLATE pg_catalog."default",
    entity_type character varying(255) COLLATE pg_catalog."default",
    leadership character varying COLLATE pg_catalog."default",
    birth_date character varying COLLATE pg_catalog."default",
    CONSTRAINT clients_pkey PRIMARY KEY (base_entity_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.client
    OWNER to postgres;