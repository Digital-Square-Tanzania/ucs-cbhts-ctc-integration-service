-- Table: public.hivst_issue_kits

-- DROP TABLE IF EXISTS public.hivst_issue_kits;

CREATE TABLE IF NOT EXISTS public.hivst_issue_kits
(
    event_id character varying COLLATE pg_catalog."default" NOT NULL,
    event_date character varying COLLATE pg_catalog."default",
    provider_id character varying COLLATE pg_catalog."default",
    location_id character varying COLLATE pg_catalog."default",
    team character varying COLLATE pg_catalog."default",
    team_id character varying COLLATE pg_catalog."default",
    base_entity_id character varying COLLATE pg_catalog."default",
    event_type character varying COLLATE pg_catalog."default",
    pre_test_counselling_and_instructions character varying COLLATE pg_catalog."default",
    client_testing_approach character varying COLLATE pg_catalog."default",
    self_test_kit_given character varying COLLATE pg_catalog."default",
    kit_code character varying COLLATE pg_catalog."default",
    extra_kits_required character varying COLLATE pg_catalog."default",
    extra_kits_issued_for character varying COLLATE pg_catalog."default",
    sexual_partner_kit_code character varying COLLATE pg_catalog."default",
    peer_friend_kit_code character varying COLLATE pg_catalog."default",
    condoms_given character varying COLLATE pg_catalog."default",
    type_of_issued_condoms character varying COLLATE pg_catalog."default",
    number_of_male_condoms_issued character varying COLLATE pg_catalog."default",
    number_of_female_condoms_issued character varying COLLATE pg_catalog."default",
    sms_notification_service character varying COLLATE pg_catalog."default",
    collection_date character varying COLLATE pg_catalog."default",
    client_kit_batch_number character varying COLLATE pg_catalog."default",
    peer_friend_kit_batch_number character varying COLLATE pg_catalog."default",
    sexual_partner_kit_batch_number character varying COLLATE pg_catalog."default",
    client_kit_expiry_date character varying COLLATE pg_catalog."default",
    peer_friend_kit_expiry_date character varying COLLATE pg_catalog."default",
    sexual_partner_kit_expiry_date character varying COLLATE pg_catalog."default",
    CONSTRAINT hivst_issue_kits_pkey PRIMARY KEY (event_id),
    CONSTRAINT hivst_issue_kits_fk FOREIGN KEY (base_entity_id)
        REFERENCES public.client (base_entity_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.hivst_issue_kits
    OWNER to postgres;