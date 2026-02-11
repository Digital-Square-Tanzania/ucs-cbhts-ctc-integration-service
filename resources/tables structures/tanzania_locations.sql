-- Table: public.tanzania_locations

-- DROP TABLE IF EXISTS public.tanzania_locations;

CREATE TABLE IF NOT EXISTS public.tanzania_locations
(
    location_uuid character varying COLLATE pg_catalog."default" NOT NULL,
    village character varying COLLATE pg_catalog."default",
    health_facility character varying COLLATE pg_catalog."default",
    hfr_code character varying COLLATE pg_catalog."default",
    ward character varying COLLATE pg_catalog."default",
    district_council character varying COLLATE pg_catalog."default",
    district character varying COLLATE pg_catalog."default",
    region character varying COLLATE pg_catalog."default",
    zone character varying COLLATE pg_catalog."default",
    country character varying COLLATE pg_catalog."default",
    is_pepfar_site boolean,
    CONSTRAINT tanzania_locations_pkey PRIMARY KEY (location_uuid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.tanzania_locations
    OWNER to postgres;