CREATE TABLE IF NOT EXISTS public.hivst_results
(
    event_id character varying COLLATE pg_catalog."default" NOT NULL,
    event_date character varying COLLATE pg_catalog."default",
    provider_id character varying COLLATE pg_catalog."default",
    location_id character varying COLLATE pg_catalog."default",
    team character varying COLLATE pg_catalog."default",
    team_id character varying COLLATE pg_catalog."default",
    base_entity_id character varying COLLATE pg_catalog."default",
    event_type character varying COLLATE pg_catalog."default",
    kit_for character varying COLLATE pg_catalog."default",
    kit_code character varying COLLATE pg_catalog."default",
    result_reg_id character varying COLLATE pg_catalog."default",
    collection_date character varying COLLATE pg_catalog."default",
    disclose_result character varying COLLATE pg_catalog."default",
    hivst_result character varying COLLATE pg_catalog."default",
    result_date character varying COLLATE pg_catalog."default",
    register_to_hts character varying COLLATE pg_catalog."default",
    CONSTRAINT hivst_results_pkey PRIMARY KEY (event_id),
    CONSTRAINT hivst_results_fk FOREIGN KEY (base_entity_id)
        REFERENCES public.client (base_entity_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.hivst_results
    OWNER to postgres;