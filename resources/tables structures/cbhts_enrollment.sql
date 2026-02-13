-- Table: public.cbhts_enrollment

-- DROP TABLE IF EXISTS public.cbhts_enrollment;

CREATE TABLE IF NOT EXISTS public.cbhts_enrollment
(
    event_date text COLLATE pg_catalog."default",
    location_id text COLLATE pg_catalog."default",
    provider_id text COLLATE pg_catalog."default",
    team text COLLATE pg_catalog."default",
    team_id text COLLATE pg_catalog."default",
    base_entity_id text COLLATE pg_catalog."default" NOT NULL,
    event_id text COLLATE pg_catalog."default" NOT NULL,
    form_submission_id text COLLATE pg_catalog."default",
    event_type text COLLATE pg_catalog."default",
    entity_type text COLLATE pg_catalog."default",
    does_the_client_still_want_to_test text COLLATE pg_catalog."default",
    eligibility_for_testing text COLLATE pg_catalog."default",
    hts_are_one_or_both_biological_parents_of_the_child_diseased text COLLATE pg_catalog."default",
    hts_does_client_report_to_test_hiv_positive_but_no_evidence_ctc text COLLATE pg_catalog."default",
    hts_does_the_child_have_abnormal_discharge_from_the_vagina_or_p text COLLATE pg_catalog."default",
    hts_does_the_child_have_any_ulcer_sore_warts_in_his_or_her_geni text COLLATE pg_catalog."default",
    hts_does_the_child_have_burning_itching_or_pain_when_urinating_ text COLLATE pg_catalog."default",
    hts_does_the_child_have_recurrent_skin_problems text COLLATE pg_catalog."default",
    hts_does_the_client_have_any_of_the_following_general_health_as text COLLATE pg_catalog."default",
    hts_does_the_client_have_any_of_the_following_sti_symptoms_asse text COLLATE pg_catalog."default",
    hts_does_the_client_have_any_of_the_following_tuberculosis_symp text COLLATE pg_catalog."default",
    hts_has_testing_been_done_more_than_12_months_ago text COLLATE pg_catalog."default",
    hts_has_the_child_been_sexually_abused text COLLATE pg_catalog."default",
    hts_has_the_child_ever_been_admitted_to_hospital_before text COLLATE pg_catalog."default",
    hts_has_the_child_ever_received_blood_transfusion text COLLATE pg_catalog."default",
    hts_has_the_child_had_a_history_of_enlargement_of_lymph_nodes text COLLATE pg_catalog."default",
    hts_has_the_child_had_a_recurrent_illness_in_the_last_3_months text COLLATE pg_catalog."default",
    hts_has_the_child_had_cough_for_two_weeks_or_more text COLLATE pg_catalog."default",
    hts_has_the_child_had_fever_for_two_weeks_or_more text COLLATE pg_catalog."default",
    hts_has_the_child_had_night_sweats_for_any_duration text COLLATE pg_catalog."default",
    hts_has_the_child_had_noticeable_weight_loss text COLLATE pg_catalog."default",
    hts_has_the_client_ever_tested_for_hiv text COLLATE pg_catalog."default",
    hts_has_the_client_had_more_than_one_sexual_partner_in_the_last text COLLATE pg_catalog."default",
    hts_has_the_client_had_sex_under_the_influence_of_drugs_or_alco text COLLATE pg_catalog."default",
    hts_has_the_client_had_sexual_intercourse_with_a_person_with_hi text COLLATE pg_catalog."default",
    hts_is_client_breastfeeding text COLLATE pg_catalog."default",
    hts_is_client_pregnant text COLLATE pg_catalog."default",
    hts_is_the_biological_mother_of_the_child_hiv_positive text COLLATE pg_catalog."default",
    hts_is_the_child_sexually_active text COLLATE pg_catalog."default",
    hts_last_hiv_test_results_are_unknown_or_were_indeterminate_inc text COLLATE pg_catalog."default",
    prompt_for_clients_with_lower_hiv_risk text COLLATE pg_catalog."default",
    prompt_for_eligibility_to_test_for_hiv text COLLATE pg_catalog."default",
    date_created bigint,
    CONSTRAINT cbhts_enrollment_pkey PRIMARY KEY (event_id)
    )

    TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cbhts_enrollment
    OWNER to postgres;