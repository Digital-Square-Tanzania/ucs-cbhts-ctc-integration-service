package com.abt.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LtfClientRequest extends BaseRequest {
    @JsonProperty("ctc_number")
    private String ctcNumber;

    @JsonProperty("base_entity_id")
    private String baseEntityId;

    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("on_art")
    private boolean onArt;

    @JsonProperty("hfr_code")
    private String hfrCode;

    @JsonProperty("followup_status")
    private String followupStatus;

    @JsonProperty("service_type")
    private String serviceType;

    @JsonProperty("last_appointment_date")
    private String lastAppointmentDate;

    @JsonProperty("client_first_name")
    private String clientFirstName;

    @JsonProperty("client_middle_name")
    private String clientMiddleName;

    @JsonProperty("client_last_name")
    private String clientLastName;

    @JsonProperty("client_phone_number")
    private String clientPhoneNumber;

    @JsonProperty("client_sex")
    private String clientSex;

    @JsonProperty("client_dob")
    private String clientDob;

    @JsonProperty("treatment_supporter_first_name")
    private String treatmentSupporterFirstName;

    @JsonProperty("treatment_supporter_middle_name")
    private String treatmentSupporterMiddleName;

    @JsonProperty("treatment_supporter_last_name")
    private String treatmentSupporterLastName;

    @JsonProperty("treatment_supporter_phone_number")
    private String treatmentSupporterPhoneNumber;

    @JsonProperty("ward")
    private String ward;

    @JsonProperty("village_street")
    private String villageStreet;

    @JsonProperty("village_or_street_chairperson_first_name")
    private String villageStreetChairPersonFirstName;

    @JsonProperty("village_or_street_chairperson_middle_name")
    private String villageStreetChairPersonMiddleName;

    @JsonProperty("village_or_street_chairperson_last_name")
    private String villageStreetChairPersonLastName;

    @JsonProperty("village_or_street_chairperson_phone_number")
    private String villageStreetChairPersonPhoneNumber;

    @JsonProperty("tencell_leader_first_name")
    private String tenCellLeaderFirstName;

    @JsonProperty("tencell_leader_middle_name")
    private String tenCellLeaderMiddleName;

    @JsonProperty("tencell_leader_last_name")
    private String tenCellLeaderLastName;

    @JsonProperty("tencell_leader_phone_number")
    private String tenCellLeaderPhoneNumber;

    public String getCtcNumber() {
        return ctcNumber;
    }

    public void setCtcNumber(String ctcNumber) {
        this.ctcNumber = ctcNumber;
    }

    public String getBaseEntityId() {
        return baseEntityId;
    }

    public void setBaseEntityId(String baseEntityId) {
        this.baseEntityId = baseEntityId;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public boolean isOnArt() {
        return onArt;
    }

    public void setOnArt(boolean onArt) {
        this.onArt = onArt;
    }

    public String getHfrCode() {
        return hfrCode;
    }

    public void setHfrCode(String hfrCode) {
        this.hfrCode = hfrCode;
    }

    public String getFollowupStatus() {
        return followupStatus;
    }

    public void setFollowupStatus(String followupStatus) {
        this.followupStatus = followupStatus;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getLastAppointmentDate() {
        return lastAppointmentDate;
    }

    public void setLastAppointmentDate(String lastAppointmentDate) {
        this.lastAppointmentDate = lastAppointmentDate;
    }

    public String getClientFirstName() {
        return clientFirstName;
    }

    public void setClientFirstName(String clientFirstName) {
        this.clientFirstName = clientFirstName;
    }

    public String getClientMiddleName() {
        return clientMiddleName;
    }

    public void setClientMiddleName(String clientMiddleName) {
        this.clientMiddleName = clientMiddleName;
    }

    public String getClientLastName() {
        return clientLastName;
    }

    public void setClientLastName(String clientLastName) {
        this.clientLastName = clientLastName;
    }

    public String getClientPhoneNumber() {
        return clientPhoneNumber;
    }

    public void setClientPhoneNumber(String clientPhoneNumber) {
        this.clientPhoneNumber = clientPhoneNumber;
    }

    public String getClientSex() {
        return clientSex;
    }

    public void setClientSex(String clientSex) {
        this.clientSex = clientSex;
    }

    public String getClientDob() {
        return clientDob;
    }

    public void setClientDob(String clientDob) {
        this.clientDob = clientDob;
    }

    public String getTreatmentSupporterFirstName() {
        return treatmentSupporterFirstName;
    }

    public void setTreatmentSupporterFirstName(String treatmentSupporterFirstName) {
        this.treatmentSupporterFirstName = treatmentSupporterFirstName;
    }

    public String getTreatmentSupporterMiddleName() {
        return treatmentSupporterMiddleName;
    }

    public void setTreatmentSupporterMiddleName(String treatmentSupporterMiddleName) {
        this.treatmentSupporterMiddleName = treatmentSupporterMiddleName;
    }

    public String getTreatmentSupporterLastName() {
        return treatmentSupporterLastName;
    }

    public void setTreatmentSupporterLastName(String treatmentSupporterLastName) {
        this.treatmentSupporterLastName = treatmentSupporterLastName;
    }

    public String getTreatmentSupporterPhoneNumber() {
        return treatmentSupporterPhoneNumber;
    }

    public void setTreatmentSupporterPhoneNumber(String treatmentSupporterPhoneNumber) {
        this.treatmentSupporterPhoneNumber = treatmentSupporterPhoneNumber;
    }

    public String getWard() {
        return ward;
    }

    public void setWard(String ward) {
        this.ward = ward;
    }

    public String getVillageStreet() {
        return villageStreet;
    }

    public void setVillageStreet(String villageStreet) {
        this.villageStreet = villageStreet;
    }

    public String getVillageStreetChairPersonFirstName() {
        return villageStreetChairPersonFirstName;
    }

    public void setVillageStreetChairPersonFirstName(String villageStreetChairPersonFirstName) {
        this.villageStreetChairPersonFirstName =
                villageStreetChairPersonFirstName;
    }

    public String getVillageStreetChairPersonMiddleName() {
        return villageStreetChairPersonMiddleName;
    }

    public void setVillageStreetChairPersonMiddleName(String villageStreetChairPersonMiddleName) {
        this.villageStreetChairPersonMiddleName =
                villageStreetChairPersonMiddleName;
    }

    public String getVillageStreetChairPersonLastName() {
        return villageStreetChairPersonLastName;
    }

    public void setVillageStreetChairPersonLastName(String villageStreetChairPersonLastName) {
        this.villageStreetChairPersonLastName =
                villageStreetChairPersonLastName;
    }

    public String getVillageStreetChairPersonPhoneNumber() {
        return villageStreetChairPersonPhoneNumber;
    }

    public void setVillageStreetChairPersonPhoneNumber(String villageStreetChairPersonPhoneNumber) {
        this.villageStreetChairPersonPhoneNumber =
                villageStreetChairPersonPhoneNumber;
    }

    public String getTenCellLeaderFirstName() {
        return tenCellLeaderFirstName;
    }

    public void setTenCellLeaderFirstName(String tenCellLeaderFirstName) {
        this.tenCellLeaderFirstName = tenCellLeaderFirstName;
    }

    public String getTenCellLeaderMiddleName() {
        return tenCellLeaderMiddleName;
    }

    public void setTenCellLeaderMiddleName(String tenCellLeaderMiddleName) {
        this.tenCellLeaderMiddleName = tenCellLeaderMiddleName;
    }

    public String getTenCellLeaderLastName() {
        return tenCellLeaderLastName;
    }

    public void setTenCellLeaderLastName(String tenCellLeaderLastName) {
        this.tenCellLeaderLastName = tenCellLeaderLastName;
    }

    public String getTenCellLeaderPhoneNumber() {
        return tenCellLeaderPhoneNumber;
    }

    public void setTenCellLeaderPhoneNumber(String tenCellLeaderPhoneNumber) {
        this.tenCellLeaderPhoneNumber = tenCellLeaderPhoneNumber;
    }
}