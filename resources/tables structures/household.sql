-- Table: public.household

-- DROP TABLE IF EXISTS public.household;

CREATE TABLE IF NOT EXISTS public.household
(
    base_entity_id character varying COLLATE pg_catalog."default" NOT NULL,
    client_id character varying COLLATE pg_catalog."default" NOT NULL,
    first_name character varying COLLATE pg_catalog."default",
    middle_name character varying COLLATE pg_catalog."default",
    last_name character varying COLLATE pg_catalog."default",
    type character varying COLLATE pg_catalog."default",
    client_type character varying COLLATE pg_catalog."default",
    family_location_name character varying COLLATE pg_catalog."default",
    gps character varying COLLATE pg_catalog."default",
    service_provider character varying COLLATE pg_catalog."default",
    location_id character varying COLLATE pg_catalog."default",
    provider_id character varying COLLATE pg_catalog."default",
    event_date timestamp without time zone,
    primary_caregiver character varying COLLATE pg_catalog."default",
    CONSTRAINT households_pkey PRIMARY KEY (base_entity_id)
    )

    TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.household
    OWNER to postgres;