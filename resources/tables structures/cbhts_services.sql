-- Table: public.cbhts_services

-- DROP TABLE IF EXISTS public.cbhts_services;

CREATE TABLE IF NOT EXISTS public.cbhts_services
(
    event_date text COLLATE pg_catalog."default",
    visit_date text COLLATE pg_catalog."default",
    hts_visit_date text COLLATE pg_catalog."default",
    location_id text COLLATE pg_catalog."default",
    provider_id text COLLATE pg_catalog."default",
    team text COLLATE pg_catalog."default",
    team_id text COLLATE pg_catalog."default",
    base_entity_id text COLLATE pg_catalog."default" NOT NULL,
    event_id text COLLATE pg_catalog."default" NOT NULL,
    form_submission_id text COLLATE pg_catalog."default",
    hts_visit_group text COLLATE pg_catalog."default",
    event_type text COLLATE pg_catalog."default",
    entity_type text COLLATE pg_catalog."default",
    hts_visit_type text COLLATE pg_catalog."default",
    hts_has_the_client_recently_tested_with_hivst text COLLATE pg_catalog."default",
    hts_previous_hivst_client_type text COLLATE pg_catalog."default",
    hts_previous_hivst_test_type text COLLATE pg_catalog."default",
    hts_client_type text COLLATE pg_catalog."default",
    hts_preventive_services text COLLATE pg_catalog."default",
    hts_testing_approach text COLLATE pg_catalog."default",
    hts_testing_point text COLLATE pg_catalog."default",
    hts_were_condoms_distributed text COLLATE pg_catalog."default",
    hts_condom_distribution text COLLATE pg_catalog."default",
    hts_number_of_male_condoms_provided integer,
    hts_number_of_female_condoms_provided integer,
    hts_has_pre_test_counselling_been_provided text COLLATE pg_catalog."default",
    hts_has_post_test_counselling_been_provided text COLLATE pg_catalog."default",
    hts_type_of_counselling_provided text COLLATE pg_catalog."default",
    hts_hiv_results_disclosure text COLLATE pg_catalog."default",
    other_people_results_have_been_disclosed_to text COLLATE pg_catalog."default",
    reasons_for_not_disclosing_results text COLLATE pg_catalog."default",
    hts_clients_tb_screening_outcome text COLLATE pg_catalog."default",
    hts_does_client_need_hiv_self_test_kits text COLLATE pg_catalog."default",
    pre_test_services_completion_status text COLLATE pg_catalog."default",
    date_created bigint,
    CONSTRAINT cbhts_services_pkey PRIMARY KEY (event_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cbhts_services
    OWNER to postgres;