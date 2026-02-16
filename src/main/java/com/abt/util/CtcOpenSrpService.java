package com.abt.util;


import com.abt.domain.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.abt.UcsCbhtsCtsIntegrationServiceApp.SECRETE_KEY;
import static com.abt.util.Utils.decryptDataNew;

/**
 * Service class for OpenSRP operations.
 */
public class CtcOpenSrpService {
    private final static Logger log =
            LoggerFactory.getLogger(CtcOpenSrpService.class);

    private static final int clientDatabaseVersion = 17;
    private static final int clientApplicationVersion = 2;
    private static final String EVENT_ADD_PATH = "/opensrp/rest/event/add";
    private static final String TASK_CREATE_PATH = "/opensrp/rest/task";
    private static final String UNIQUE_IDS_PATH_PREFIX = "/opensrp/uniqueids/get?source=2&numberToGenerate=";
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat(
            "yyyy-MM-dd");

    private static final Map<String, String> relationshipMap = new HashMap<>();
    private static final Map<String, String> notificationMethodMap =
            new HashMap<>();

    private static Gson gson =
            new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss" +
                    ".SSSZ").registerTypeAdapter(org.joda.time.DateTime.class, new DateTimeTypeConverter()).create();


    // Static block to initialize the map
    static {
        relationshipMap.put("SP", "sexual_partner");
        relationshipMap.put("NP", "needle_sharing_partner");
        relationshipMap.put("BC", "biological_child");
        relationshipMap.put("Sib", "siblings");
        relationshipMap.put("BM", "biological_mother");
        relationshipMap.put("BF", "biological_father");

        notificationMethodMap.put("CR", "client_referral");
        notificationMethodMap.put("PR", "provider_referral");
        notificationMethodMap.put("CT", "contract_referral");
        notificationMethodMap.put("DR", "dual_referral");
    }

    private static String getRelationshipDescription(String code) {
        return relationshipMap.getOrDefault(code, "na");
    }

    private static String getNotificationMethod(String code) {
        return notificationMethodMap.getOrDefault(code, "na");
    }


    /**
     * Creates and returns an observation for the start event.
     *
     * @return Obs object for the start event.
     */
    private static Obs getStartOb() {
        return new Obs("concept", "start",
                "163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "",
                Arrays.asList(new Object[]{new Date()}), null, null, "start");
    }

    /**
     * Creates and returns an observation for the end event.
     *
     * @return Obs object for the end event.
     */
    private static Obs getEndOb() {
        return new Obs("concept", "end",
                "163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "",
                Arrays.asList(new Object[]{new Date()}), null, null, "end");
    }

    /**
     * Creates and returns an observation for the recGUID event.
     *
     * @return Obs object for the end event.
     */
    private static Obs getRecGuidOb(BaseRequest baseRequest) {
        return new Obs("concept", "rec_guid",
                "rec_guid", "",
                Arrays.asList(new Object[]{baseRequest.getRecGuid()}), null, null, "rec_guid");
    }


    /**
     * Creates a Client object for a given CTCPatient.
     *
     * @param indexContacts The IndexContactRequest object.
     * @return Client object for the family.
     */
    public static Client getClientEvent(IndexContactRequest indexContacts) {
        Client familyClient = new Client(UUID.randomUUID().toString());
        familyClient.setFirstName(decryptDataNew(indexContacts.getLastName(), SECRETE_KEY, null));
        familyClient.setLastName("Family");
        familyClient.setBirthdate(new Date(0));
        familyClient.setBirthdateApprox(false);
        familyClient.setDeathdateApprox(false);
        familyClient.setGender(indexContacts.getSex().equalsIgnoreCase("M") ?
                "Male" : "Female");
        familyClient.setClientApplicationVersion(clientApplicationVersion);
        familyClient.setClientDatabaseVersion(clientDatabaseVersion);
        familyClient.setType("Client");
        familyClient.setId(UUID.randomUUID().toString());
        familyClient.setDateCreated(new Date());
        familyClient.setAttributes(new HashMap<>());


        return familyClient;
    }

    /**
     * Creates a Client object for a given CTCPatient.
     *
     * @param ltfClientRequest The LtfClientRequest object.
     * @return Client object for the family.
     */
    public static Client getClientEvent(LtfClientRequest ltfClientRequest) {
        Client familyClient = new Client(UUID.randomUUID().toString());
        familyClient.setFirstName(decryptDataNew(ltfClientRequest.getClientLastName(), SECRETE_KEY, null));
        familyClient.setLastName("Family");
        familyClient.setBirthdate(new Date(0));
        familyClient.setBirthdateApprox(false);
        familyClient.setDeathdateApprox(false);
        familyClient.setGender(ltfClientRequest.getClientSex().equalsIgnoreCase("M") ?
                "Male" : "Female");
        familyClient.setClientApplicationVersion(clientApplicationVersion);
        familyClient.setClientDatabaseVersion(clientDatabaseVersion);
        familyClient.setType("Client");
        familyClient.setId(UUID.randomUUID().toString());
        familyClient.setDateCreated(new Date());
        familyClient.setAttributes(new HashMap<>());


        return familyClient;
    }

    /**
     * Sets the address for the given Client object.
     *
     * @param client  The Client object.
     * @param village The village information.
     * @param mapCue  The mapCue information.
     */
    public static void setAddress(Client client, String village,
                                  String mapCue) {
        List<Address> addresses = new ArrayList<>();

        if (village != null && !village.isEmpty()) {
            Address villageAddress = new Address();
            villageAddress.setAddressType("village");
            villageAddress.setCityVillage(village);

            if (mapCue != null && !mapCue.isEmpty()) {
                HashMap<String, String> addressFields = new HashMap<>();
                addressFields.put("landmark", mapCue);
                villageAddress.setAddressFields(addressFields);
            } else {
                villageAddress.setAddressFields(new HashMap<>());
            }
            addresses.add(villageAddress);
        }

        client.setAddresses(addresses);
    }

    /**
     * Creates a Client object for the family head based on a given CTCPatient.
     *
     * @param indexContacts The IndexContactRequest object.
     * @return Client object for the family head.
     */
    public static Client getFamilyHeadClientEvent(IndexContactRequest indexContacts) {
        Client ctcClient;
        if (indexContacts.getBaseEntityId() == null) {
            ctcClient = new Client(UUID.randomUUID().toString());
        } else {
            ctcClient = new Client(indexContacts.getBaseEntityId());
        }

        try {
            ctcClient.setFirstName(decryptDataNew(indexContacts.getFirstName(), SECRETE_KEY, null));
            ctcClient.setMiddleName(decryptDataNew(indexContacts.getMiddleName(), SECRETE_KEY, null));
        } catch (Exception e) {
            ctcClient.setMiddleName("");
        }

        ctcClient.setLastName(decryptDataNew(indexContacts.getLastName(), SECRETE_KEY, null));
        ctcClient.setGender(indexContacts.getSex().equalsIgnoreCase("M") ?
                "Male" : "Female");

        try {
            ctcClient.setBirthdate(inputFormat.parse(indexContacts.getDob()));
            ctcClient.setBirthdateApprox(false);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        ctcClient.setType("Client");
        ctcClient.setDeathdateApprox(false);
        ctcClient.setClientApplicationVersion(clientApplicationVersion);
        ctcClient.setClientDatabaseVersion(clientDatabaseVersion);

        Map<String, Object> attributes = new HashMap<>();
        List<String> id_available = new ArrayList<>();
        id_available.add("chk_none");
        attributes.put("id_avail", new Gson().toJson(id_available));
        attributes.put("Community_Leader", new Gson().toJson(id_available));
        attributes.put("Health_Insurance_Type", "None");

        ctcClient.setAttributes(attributes);
        return ctcClient;
    }

    /**
     * Creates a Client object for the family head based on a given CTCPatient.
     *
     * @param ltfClientRequest The LtfClientRequest object.
     * @return Client object for the family head.
     */
    public static Client getFamilyHeadClientEvent(LtfClientRequest ltfClientRequest) {
        Client ctcClient;
        if (ltfClientRequest.getBaseEntityId() == null) {
            ctcClient = new Client(UUID.randomUUID().toString());
        } else {
            ctcClient = new Client(ltfClientRequest.getBaseEntityId());
        }

        try {
            ctcClient.setFirstName(decryptDataNew(ltfClientRequest.getClientFirstName(), SECRETE_KEY, null));
            ctcClient.setMiddleName(decryptDataNew(ltfClientRequest.getClientMiddleName(), SECRETE_KEY, null));
        } catch (Exception e) {
            ctcClient.setMiddleName("");
        }

        ctcClient.setLastName(decryptDataNew(ltfClientRequest.getClientLastName(), SECRETE_KEY, null));
        ctcClient.setGender(ltfClientRequest.getClientSex().equalsIgnoreCase(
                "M") ?
                "Male" : "Female");

        try {
            ctcClient.setBirthdate(inputFormat.parse(ltfClientRequest.getClientDob()));
            ctcClient.setBirthdateApprox(false);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        ctcClient.setType("Client");
        ctcClient.setDeathdateApprox(false);
        ctcClient.setClientApplicationVersion(clientApplicationVersion);
        ctcClient.setClientDatabaseVersion(clientDatabaseVersion);

        Map<String, Object> attributes = new HashMap<>();
        List<String> id_available = new ArrayList<>();
        id_available.add("chk_none");
        attributes.put("id_avail", new Gson().toJson(id_available));
        attributes.put("Community_Leader", new Gson().toJson(id_available));
        attributes.put("Health_Insurance_Type", "None");

        ctcClient.setAttributes(attributes);
        return ctcClient;
    }

    /**
     * Creates a Family Registration Event for a given Client and CTCPatient.
     *
     * @param client       The Client object.
     * @param indexContact The IndexContactRequest object.
     * @return Family Registration Event.
     */
    public static Event getFamilyRegistrationEvent(Client client,
                                                   IndexContactRequest indexContact) {
        Event familyRegistrationEvent = new Event();
        familyRegistrationEvent.setBaseEntityId(client.getBaseEntityId());
        familyRegistrationEvent.setEventType("Family Registration");
        familyRegistrationEvent.setEntityType("ec_independent_client");
        setMetaData(familyRegistrationEvent, indexContact);
        familyRegistrationEvent.addObs(new Obs("formsubmissionField", "text",
                "last_interacted_with", "",
                Arrays.asList(new Object[]{String.valueOf(Calendar.getInstance().getTimeInMillis())}), null, null, "last_interacted_with"));
        return familyRegistrationEvent;
    }

    /**
     * Creates a Family Registration Event for a given Client and CTCPatient.
     *
     * @param client           The Client object.
     * @param ltfClientRequest The LtfClientRequest object.
     * @return Family Registration Event.
     */
    public static Event getFamilyRegistrationEvent(Client client,
                                                   LtfClientRequest ltfClientRequest) {
        Event familyRegistrationEvent = new Event();
        familyRegistrationEvent.setBaseEntityId(client.getBaseEntityId());
        familyRegistrationEvent.setEventType("Family Registration");
        familyRegistrationEvent.setEntityType("ec_independent_client");
        setMetaData(familyRegistrationEvent, ltfClientRequest);
        familyRegistrationEvent.addObs(new Obs("formsubmissionField", "text",
                "last_interacted_with", "",
                Arrays.asList(new Object[]{String.valueOf(Calendar.getInstance().getTimeInMillis())}), null, null, "last_interacted_with"));
        return familyRegistrationEvent;
    }


    /**
     * Creates a Family Member Registration Event for a given Client and
     * CTCPatient.
     *
     * @param client              The Client object.
     * @param indexContactRequest The IndexContactRequest object.
     * @return Family Member Registration Event.
     */
    public static Event getFamilyMemberRegistrationEvent(Client client,
                                                         IndexContactRequest indexContactRequest) {
        Event familyMemberRegistrationEvent = new Event();
        familyMemberRegistrationEvent.setBaseEntityId(client.getBaseEntityId());
        familyMemberRegistrationEvent.setEventType("Family Member " +
                "Registration");
        familyMemberRegistrationEvent.setEntityType("ec_independent_client");
        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "id_avail", "", Arrays.asList(new Object[]{"None"}),
                null, null, "id_avail"));

        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "leader", "", Arrays.asList(new Object[]{"None"}),
                null, null, "leader"));

        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "last_interacted_with", "",
                Arrays.asList(new Object[]{String.valueOf(Calendar.getInstance().getTimeInMillis())}), null, null, "last_interacted_with"));

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "surname", "", Arrays.asList(new Object[]{decryptDataNew(client.getLastName(), SECRETE_KEY, null)}), null,
                null, "surname"));

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "phone_number", "",
                Arrays.asList(new Object[]{decryptDataNew(indexContactRequest.getPrimaryPhoneNumber(), SECRETE_KEY, null)}), null,
                null, "phone_number"));

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "other_phone_number",
                "",
                Arrays.asList(new Object[]{decryptDataNew(indexContactRequest.getAlternativePhoneNumber(), SECRETE_KEY, null)}), null,
                null, "other_phone_number"));

        PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
        try {
            Phonenumber.PhoneNumber tzPhoneNumber =
                    phoneUtil.parse(decryptDataNew(indexContactRequest.getPrimaryPhoneNumber(), SECRETE_KEY, null), "TZ");

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "phone_number", "",
                    Arrays.asList(new Object[]{phoneUtil.format(tzPhoneNumber
                            , PhoneNumberUtil.PhoneNumberFormat.NATIONAL).replace(" ", "")}), null, null, "phone_number"));
        } catch (NumberParseException e) {
            System.err.println("NumberParseException was thrown: " + e);
        }

        try {
            Phonenumber.PhoneNumber cateTakerPhoneNumber =
                    phoneUtil.parse(decryptDataNew(indexContactRequest.getAlternativePhoneNumber(), SECRETE_KEY, null), "TZ");

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "other_phone_number", "",
                    Arrays.asList(new Object[]{phoneUtil.format(cateTakerPhoneNumber, PhoneNumberUtil.PhoneNumberFormat.NATIONAL).replace(" ", "")}), null, null, "other_phone_number"));
        } catch (NumberParseException e) {
            System.err.println("NumberParseException was thrown: " + e.toString());
        }

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "data_source", "", Arrays.asList(new Object[]{"ctc_import"}),
                null, null, "data_source"));


        setMetaData(familyMemberRegistrationEvent, indexContactRequest);
        return familyMemberRegistrationEvent;
    }


    /**
     * Creates a Family Member Registration Event for a given Client and
     * CTCPatient.
     *
     * @param client           The Client object.
     * @param ltfClientRequest The LtfClientRequest object.
     * @return Family Member Registration Event.
     */
    public static Event getFamilyMemberRegistrationEvent(Client client,
                                                         LtfClientRequest ltfClientRequest) {
        Event familyMemberRegistrationEvent = new Event();
        familyMemberRegistrationEvent.setBaseEntityId(client.getBaseEntityId());
        familyMemberRegistrationEvent.setEventType("Family Member Registration");
        familyMemberRegistrationEvent.setEntityType("ec_independent_client");
        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "id_avail", "", Arrays.asList(new Object[]{"None"}),
                null, null, "id_avail"));
        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "leader", "", Arrays.asList(new Object[]{"None"}),
                null, null, "leader"));
        familyMemberRegistrationEvent.addObs(new Obs("formsubmissionField",
                "text", "last_interacted_with", "",
                Arrays.asList(new Object[]{String.valueOf(Calendar.getInstance().getTimeInMillis())}),
                null, null, "last_interacted_with"));

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "surname",
                "", Arrays.asList(new Object[]{client.getLastName()}), null,
                null, "surname"));

        if (StringUtils.isNotBlank(ltfClientRequest.getVillageStreetChairPersonFirstName())) {
            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "chairperson_name",
                    "",
                    Arrays.asList(new Object[]{Utils.concatenateFullName(
                            decryptDataNew(ltfClientRequest.getVillageStreetChairPersonFirstName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getVillageStreetChairPersonMiddleName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getVillageStreetChairPersonLastName(), SECRETE_KEY, null)
                    )}),
                    null,
                    null, "chairperson_name"));

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "chairperson_phone",
                    "",
                    Arrays.asList(new Object[]{
                            decryptDataNew(ltfClientRequest.getVillageStreetChairPersonPhoneNumber(), SECRETE_KEY, null)
                    }), null, null, "chairperson_phone"));
        }

        if (StringUtils.isNotBlank(ltfClientRequest.getTenCellLeaderFirstName())) {
            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "ten_house_cell_leader_name",
                    "",
                    Arrays.asList(new Object[]{Utils.concatenateFullName(
                            decryptDataNew(ltfClientRequest.getTenCellLeaderFirstName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getTenCellLeaderMiddleName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getTenCellLeaderLastName(), SECRETE_KEY, null)
                    )}),
                    null,
                    null, "ten_house_cell_leader_name"));

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "ten_house_cell_leader_phone", "",
                    Arrays.asList(new Object[]{
                            decryptDataNew(ltfClientRequest.getTenCellLeaderPhoneNumber(), SECRETE_KEY, null)
                    }),
                    null, null, "ten_house_cell_leader_phone"));
        }

        if (StringUtils.isNotBlank(ltfClientRequest.getTreatmentSupporterFirstName())) {
            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "primary_caregiver_name",
                    "",
                    Arrays.asList(new Object[]{Utils.concatenateFullName(decryptDataNew(ltfClientRequest.getTreatmentSupporterFirstName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getTreatmentSupporterMiddleName(), SECRETE_KEY, null),
                            decryptDataNew(ltfClientRequest.getTreatmentSupporterLastName(), SECRETE_KEY, null)
                    )}),
                    null,
                    null, "primary_caregiver_name"));

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "other_phone_number",
                    "",
                    Arrays.asList(new Object[]{
                            decryptDataNew(ltfClientRequest.getTreatmentSupporterPhoneNumber(), SECRETE_KEY, null)}),
                    null,
                    null, "other_phone_number"));
        }

        PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
        try {
            Phonenumber.PhoneNumber tzPhoneNumber =
                    phoneUtil.parse(decryptDataNew(ltfClientRequest.getClientPhoneNumber(), SECRETE_KEY, null),
                            "TZ");

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "phone_number", "",
                    Arrays.asList(new Object[]{phoneUtil.format(tzPhoneNumber
                            , PhoneNumberUtil.PhoneNumberFormat.NATIONAL).replace(" ", "")}), null, null, "phone_number"));
        } catch (NumberParseException e) {
            System.err.println("NumberParseException was thrown: " + e);
        }

        try {
            Phonenumber.PhoneNumber cateTakerPhoneNumber =
                    phoneUtil.parse(decryptDataNew(ltfClientRequest.getTreatmentSupporterPhoneNumber(), SECRETE_KEY, null), "TZ");

            familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                    "other_phone_number", "",
                    Arrays.asList(new Object[]{phoneUtil.format(cateTakerPhoneNumber, PhoneNumberUtil.PhoneNumberFormat.NATIONAL).replace(" ", "")}), null, null, "other_phone_number"));
        } catch (NumberParseException e) {
            System.err.println("NumberParseException was thrown: " + e.toString());
        }

        familyMemberRegistrationEvent.addObs(new Obs("concept", "text",
                "data_source", "", Arrays.asList(new Object[]{"ctc_import"}),
                null, null, "data_source"));


        setMetaData(familyMemberRegistrationEvent, ltfClientRequest);
        return familyMemberRegistrationEvent;
    }


    /**
     * Creates an Index Contact  Elicitation Event for a given Client
     *
     * @param client              The Client object.
     * @param indexContactRequest The IndexContactRequest object.
     * @return Index Contact Elicitation Event.
     */
    public static Event getIndexContactElicitationEvent(Client client,
                                                        IndexContactRequest indexContactRequest) {
        Event hivFollowupEvent = new Event();
        hivFollowupEvent.setBaseEntityId(client.getBaseEntityId());
        hivFollowupEvent.setEventType("Hiv Index Contact Registration");
        hivFollowupEvent.setEntityType("ec_hiv_index_hf");

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "how_to_notify_the_contact_client", "",
                Arrays.asList(new Object[]{getNotificationMethod(indexContactRequest.getNotificationMethod())}),
                null, null, "how_to_notify_the_contact_client"));

        hivFollowupEvent.addObs(new Obs("concept", "text", "relationship", "",
                Arrays.asList(new Object[]{getRelationshipDescription(indexContactRequest.getRelationship())}),
                null, null, "relationship"));

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "elicitation_number", "",
                Arrays.asList(new Object[]{indexContactRequest.getCtcUniqueId()}), null, null, "elicitation_number"));

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "index_client_base_entity_id", "",
                Arrays.asList(new Object[]{indexContactRequest.getCtcNumber()}), null, null, "index_client_base_entity_id"));

        setMetaData(hivFollowupEvent, indexContactRequest);
        return hivFollowupEvent;
    }


    /**
     * Creates an Index Contact  Community Followup Event for a given Client
     *
     * @param client              The Client object.
     * @param indexContactRequest The IndexContactRequest object.
     * @return Index Contact Elicitation Event.
     */
    public static Event getIndexContactCommunityFollowupEvent(Client client,
                                                              IndexContactRequest indexContactRequest) {
        Event hivFollowupEvent = new Event();
        hivFollowupEvent.setBaseEntityId(client.getBaseEntityId());
        hivFollowupEvent.setEventType("HIV Index Contact Community Followup " +
                "Referral");
        hivFollowupEvent.setEntityType(
                "ec_hiv_index_contact_community_followup");

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "refer_to_chw", "",
                Arrays.asList(new Object[]{"Yes"}),
                null, null, "refer_to_chw"));

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "hiv_index_contact_community_followup_referral_date", "",
                Arrays.asList(new Object[]{getRelationshipDescription(indexContactRequest.getElicitationDate())}),
                null, null,
                "hiv_index_contact_community_followup_referral_date"));

        hivFollowupEvent.addObs(new Obs("concept", "text",
                "chw_referral_hf", "",
                Arrays.asList(new Object[]{indexContactRequest.getLocationId()}), null, null, "chw_referral_hf"));

        setMetaData(hivFollowupEvent, indexContactRequest);
        return hivFollowupEvent;
    }


    /**
     * Creates an LTF followup Event for a given Client
     *
     * @param client           The Client object.
     * @param ltfClientRequest The LtfClientRequest object.
     * @return LTF Event.
     */
    public static Event getLtfEvent(Client client,
                                    LtfClientRequest ltfClientRequest) {
        Event ltfEvent = new Event();
        ltfEvent.setBaseEntityId(client.getBaseEntityId());
        ltfEvent.setEventType("Referral Registration");
        ltfEvent.setEntityType("ec_referral");

        ltfEvent.addObs(new Obs("concept", "text",
                "on_art", "",
                Arrays.asList(new Object[]{"yes"}), null, null, "on_art"));

        ltfEvent.addObs(new Obs("concept", "text", "chw_referral_hf", "",
                Arrays.asList(new Object[]{ltfClientRequest.getLocationId()}),
                null, null, "chw_referral_hf"));

        ltfEvent.addObs(new Obs("concept", "text", "chw_referral_service", "",
                Arrays.asList(new Object[]{"LTFU"}),
                null, null, "chw_referral_service"));

        ltfEvent.addObs(new Obs("concept", "text",
                "problem", "",
                Arrays.asList(new Object[]{"CTC"}), null, null, "problem"));

        ltfEvent.addObs(new Obs("concept", "text",
                "last_appointment_date", "",
                Arrays.asList(new Object[]{ltfClientRequest.getLastAppointmentDate()}), null, null, "last_appointment_date"));

        setMetaData(ltfEvent, ltfClientRequest);
        return ltfEvent;
    }


    /**
     * Generates Client events for a list of CTCPatients.
     *
     * @param indexContactRequest List of IndexContactRequest objects.
     * @return JSON representation of ClientEvents.
     */
    public static String generateIndexClientEvent(IndexContactRequest indexContactRequest, String mUrl, String username, String password) {
        if (indexContactRequest.getBaseEntityId() == null) {
            try {
                JSONArray identifiers = fetchOpenMRSIds(mUrl, username,
                        password,
                        1);
                log.info("Received identifiers : " + identifiers.toString());
                indexContactRequest.setUniqueId(identifiers.getString(0).replace(
                        "-", ""));

                indexContactRequest.setBaseEntityId(UUID.randomUUID().toString());

                List<Client> clients = new ArrayList<>();
                List<Event> events = new ArrayList<>();

                Client familyClient = getClientEvent(indexContactRequest);
                Client indexContact =
                        getFamilyHeadClientEvent(indexContactRequest);

                indexContact.setBaseEntityId(indexContactRequest.getBaseEntityId());

                Map<String, List<String>> familyRelationships = new HashMap<>();
                familyRelationships.put("family_head",
                        Collections.singletonList(indexContactRequest.getBaseEntityId()));
                familyRelationships.put("primary_caregiver",
                        Collections.singletonList(indexContactRequest.getBaseEntityId()));
                familyClient.setRelationships(familyRelationships);

                Map<String, String> familyIdentifier = new HashMap<>();
                familyIdentifier.put("opensrp_id",
                        indexContactRequest.getUniqueId() + "_family");
                familyClient.setIdentifiers(familyIdentifier);

                Map<String, List<String>> ctcClientRelations = new HashMap<>();
                ctcClientRelations.put("family",
                        Collections.singletonList(familyClient.getBaseEntityId()));
                indexContact.setRelationships(ctcClientRelations);

                Map<String, String> clientIdentifier = new HashMap<>();
                clientIdentifier.put("opensrp_id",
                        indexContactRequest.getUniqueId());
                indexContact.setIdentifiers(clientIdentifier);


                //Generate family registration event
                Event familyRegistrationEvent =
                        getFamilyRegistrationEvent(familyClient,
                                indexContactRequest);

                //Generate family Member registration event
                Event familyMemberRegistrationEvent =
                        getFamilyMemberRegistrationEvent(indexContact,
                                indexContactRequest);


                clients.add(familyClient);
                clients.add(indexContact);

                events.add(familyRegistrationEvent);
                events.add(familyMemberRegistrationEvent);
                events.add(getIndexContactElicitationEvent(indexContact
                        , indexContactRequest));

                events.add(getIndexContactCommunityFollowupEvent(indexContact
                        , indexContactRequest));


                ClientEvents clientEvents = new ClientEvents();
                clientEvents.setClients(clients);
                clientEvents.setEvents(events);
                clientEvents.setNoOfEvents(events.size());

                Gson gson =
                        new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm" +
                                ":ss" +
                                ".SSSZ").registerTypeAdapter(org.joda.time.DateTime.class, new DateTimeTypeConverter()).create();


                return gson.toJson(clientEvents);
            } catch (Exception e) {
                log.error(e.getMessage());
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Generates Client events for a list of CTCPatients.
     *
     * @param ltfRequest List of LTF objects.
     * @return JSON representation of ClientEvents.
     */
    public static String generateLtfEvent(LtfClientRequest ltfRequest
            , String mUrl, String username, String password) {

        if (ltfRequest.getBaseEntityId() == null) {
            try {
                JSONArray identifiers = fetchOpenMRSIds(mUrl, username,
                        password,
                        1);
                log.info("Received identifiers : " + identifiers.toString());
                ltfRequest.setUniqueId(identifiers.getString(0).replace(
                        "-", ""));
                ltfRequest.setBaseEntityId(UUID.randomUUID().toString());
            } catch (Exception e) {
                log.error(e.getMessage());
                return null;
            }
        }

        List<Client> clients = new ArrayList<>();
        List<Event> events = new ArrayList<>();

        Client familyClient = getClientEvent(ltfRequest);
        Client ltfClient = getFamilyHeadClientEvent(ltfRequest);

        ltfClient.setBaseEntityId(ltfRequest.getBaseEntityId());

        Map<String, List<String>> familyRelationships = new HashMap<>();
        familyRelationships.put("family_head",
                Collections.singletonList(ltfRequest.getBaseEntityId()));
        familyRelationships.put("primary_caregiver",
                Collections.singletonList(ltfRequest.getBaseEntityId()));
        familyClient.setRelationships(familyRelationships);

        Map<String, String> familyIdentifier = new HashMap<>();
        familyIdentifier.put("opensrp_id",
                ltfRequest.getUniqueId() + "_family");
        familyClient.setIdentifiers(familyIdentifier);

        Map<String, List<String>> ctcClientRelations = new HashMap<>();
        ctcClientRelations.put("family",
                Collections.singletonList(familyClient.getBaseEntityId()));
        ltfClient.setRelationships(ctcClientRelations);

        Map<String, String> clientIdentifier = new HashMap<>();
        clientIdentifier.put("opensrp_id", ltfRequest.getUniqueId());
        ltfClient.setIdentifiers(clientIdentifier);


        //Generate family registration event
        Event familyRegistrationEvent =
                getFamilyRegistrationEvent(familyClient, ltfRequest);

        //Generate family Member registration event
        Event familyMemberRegistrationEvent =
                getFamilyMemberRegistrationEvent(ltfClient,
                        ltfRequest);

        //Generate LTF/MISSAP event
        Event ltfEvent = getLtfEvent(ltfClient, ltfRequest);

        clients.add(familyClient);
        clients.add(ltfClient);
        events.add(familyRegistrationEvent);
        events.add(familyMemberRegistrationEvent);
        events.add(ltfEvent);


        ClientEvents clientEvents = new ClientEvents();
        clientEvents.setClients(clients);
        clientEvents.setEvents(events);
        clientEvents.setNoOfEvents(events.size());

        Task task = Utils.generateTask(ltfRequest, ltfEvent.getFormSubmissionId());
        sendDataToDestination(
                gson.toJson(task),
                taskCreateUrl(mUrl),
                username,
                password,
                ltfRequest.getBaseEntityId(),
                ltfRequest.getUniqueId()
        );

        return gson.toJson(clientEvents);

    }

    /**
     * Set Event Metadata
     *
     * @param event       created Event
     * @param baseRequest Object
     */
    private static void setMetaData(Event event, BaseRequest baseRequest) {
        event.setLocationId(baseRequest.getLocationId());
        event.setProviderId(baseRequest.getProviderId());
        event.setTeamId(baseRequest.getTeamId());
        event.setTeam(baseRequest.getTeam());
        event.setType("Event");
        event.setFormSubmissionId(UUID.randomUUID().toString());
        event.setEventDate(new Date());
        event.setDateCreated(new Date());
        event.addObs(CtcOpenSrpService.getStartOb());
        event.addObs(CtcOpenSrpService.getEndOb());
        event.addObs(CtcOpenSrpService.getRecGuidOb(baseRequest));
        event.setClientApplicationVersion(clientApplicationVersion);
        event.setClientDatabaseVersion(clientDatabaseVersion);
        event.setDuration(0);
        event.setIdentifiers(new HashMap<>());
    }

    public static Response sendDataToDestination(String payload, String mUrl,
                                                 String username,
                                                 String password,
                                                 String baseEntityId,
                                                 String uniqueId) {
        Response response = new Response();
        try {
            URL url = new URL(mUrl);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; utf-8");
            conn.setRequestProperty("Accept", "application/json");
            configureBasicAuthHeader(username, password, conn);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = payload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Get the response code
            int responseCode = conn.getResponseCode();
            System.out.println("POST Response Code :: " + responseCode);

            if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) { // success
                System.out.println("POST was successful.");
                response.setDescription("sending successful");
                response.setBaseEntityId(baseEntityId);
                response.setUniqueId(uniqueId);
            } else if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                System.out.println("POST request failed.");

                response.setDescription("Authentication Error: Incorrect " +
                        "Username or " +
                        "password");
            } else {
                System.out.println("POST request failed.");
                response.setDescription("Error: Sending data to UCS " +
                        "failed");
            }

            conn.disconnect();
        } catch (Exception e) {
            LoggerFactory.getLogger(CtcOpenSrpService.class).error(e.getMessage(), e);
            response.setDescription("Error: " + e.getMessage());

        }

        return response;
    }

    public static void configureBasicAuthHeader(String username,
                                                String password,
                                                HttpURLConnection conn) {
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            String auth = username + ":" + password;
            byte[] encodedAuth =
                    Base64.getEncoder().encode(auth.getBytes(StandardCharsets.ISO_8859_1));
            String authHeader = "Basic " + new String(encodedAuth);

            conn.setRequestProperty("Authorization", authHeader);
        }
    }

    public static JSONArray fetchOpenMRSIds(String mUrl, String username,
                                            String password,
                                            int numberToGenerate) throws Exception {
        String url = uniqueIdsUrl(mUrl, numberToGenerate);
        System.out.println("URL: " + url);
        return new JSONObject(sendGetRequest(url, username, password)).getJSONArray("identifiers");
    }

    public static String sendGetRequest(String url, String username,
                                        String password) throws IOException {
        URL urlObject = new URL(url);
        HttpURLConnection connection =
                (HttpURLConnection) urlObject.openConnection();
        connection.setConnectTimeout(60000);

        // Set the request method to GET
        connection.setRequestMethod("GET");

        // Set up basic authentication
        String credentials = username + ":" + password;

        String encodedCredentials =
                new String(Base64.getEncoder().encode(credentials.getBytes(StandardCharsets.ISO_8859_1)));
        connection.setRequestProperty("Authorization",
                "Basic " + encodedCredentials);

        // Get the response code
        int responseCode = connection.getResponseCode();

        // Read the response from the server
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }

        // Close the connection
        connection.disconnect();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            return response.toString();
        } else {
            throw new IOException("Failed to get response. Response Code: " + responseCode);
        }
    }


    public static String eventAddUrl(String baseUrl) {
        return buildUrl(baseUrl, EVENT_ADD_PATH);
    }

    public static String taskCreateUrl(String baseUrl) {
        return buildUrl(baseUrl, TASK_CREATE_PATH);
    }

    public static String uniqueIdsUrl(String baseUrl, int numberToGenerate) {
        return buildUrl(baseUrl, UNIQUE_IDS_PATH_PREFIX + numberToGenerate);
    }

    private static String buildUrl(String baseUrl, String path) {
        String normalizedBaseUrl = baseUrl == null ? "" : baseUrl.trim();
        while (normalizedBaseUrl.endsWith("/")) {
            normalizedBaseUrl = normalizedBaseUrl.substring(0, normalizedBaseUrl.length() - 1);
        }

        String normalizedPath = path == null ? "" : path.trim();
        if (!normalizedPath.startsWith("/")) {
            normalizedPath = "/" + normalizedPath;
        }

        return normalizedBaseUrl + normalizedPath;
    }

}
