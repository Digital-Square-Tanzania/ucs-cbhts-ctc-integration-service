-- Table: public.team_members

-- DROP TABLE IF EXISTS public.team_members;

CREATE TABLE IF NOT EXISTS public.team_members
(
    uuid character varying COLLATE pg_catalog."default" NOT NULL,
    identifier character varying COLLATE pg_catalog."default",
    name character varying COLLATE pg_catalog."default",
    location_uuid character varying COLLATE pg_catalog."default" NOT NULL,
    location_name character varying COLLATE pg_catalog."default",
    team_name character varying COLLATE pg_catalog."default",
    CONSTRAINT team_members_pkey PRIMARY KEY (uuid, location_uuid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.team_members
    OWNER to postgres;