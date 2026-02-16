package com.abt.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexContactRequest extends BaseRequest {
    @JsonProperty("base_entity_id")
    private String baseEntityId;

    @JsonProperty("ctc_unique_id")
    private String ctcUniqueId;

    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("disclose_index")
    private String discloseIndex;

    @JsonProperty("elicitation_date")
    private String elicitationDate;

    @JsonProperty("provider_first_name")
    private String providerFirstName;

    @JsonProperty("provider_middle_name")
    private String providerMiddleName;

    @JsonProperty("provider_last_name")
    private String providerLastName;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("middle_name")
    private String middleName;

    @JsonProperty("last_name")
    private String lastName;

    @JsonProperty("sex")
    private String sex;

    @JsonProperty("dob")
    private String dob;

    @JsonProperty("primary_phone_number")
    private String primaryPhoneNumber;

    @JsonProperty("alternative_phone_number")
    private String alternativePhoneNumber;

    @JsonProperty("ipv_screening_status")
    private String ipvScreeningStatus;

    @JsonProperty("ipv_screening_outcome")
    private String ipvScreeningOutcome;

    @JsonProperty("notification_method")
    private String notificationMethod;

    @JsonProperty("relationship")
    private String relationship;

    @JsonProperty("ward")
    private String ward;

    @JsonProperty("village_street")
    private String villageStreet;

    @JsonProperty("hfr_code")
    private String hfrCode;

    @JsonProperty("ctc_number")
    private String ctcNumber;

    public String getCtcUniqueId() {
        return ctcUniqueId;
    }

    public void setCtcUniqueId(String ctcUniqueId) {
        this.ctcUniqueId = ctcUniqueId;
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

    public String getDiscloseIndex() {
        return discloseIndex;
    }

    public void setDiscloseIndex(String discloseIndex) {
        this.discloseIndex = discloseIndex;
    }

    public String getElicitationDate() {
        return elicitationDate;
    }

    public void setElicitationDate(String elicitationDate) {
        this.elicitationDate = elicitationDate;
    }

    public String getProviderFirstName() {
        return providerFirstName;
    }

    public void setProviderFirstName(String providerFirstName) {
        this.providerFirstName = providerFirstName;
    }

    public String getProviderMiddleName() {
        return providerMiddleName;
    }

    public void setProviderMiddleName(String providerMiddleName) {
        this.providerMiddleName = providerMiddleName;
    }

    public String getProviderLastName() {
        return providerLastName;
    }

    public void setProviderLastName(String providerLastName) {
        this.providerLastName = providerLastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public String getPrimaryPhoneNumber() {
        return primaryPhoneNumber;
    }

    public void setPrimaryPhoneNumber(String primaryPhoneNumber) {
        this.primaryPhoneNumber = primaryPhoneNumber;
    }

    public String getAlternativePhoneNumber() {
        return alternativePhoneNumber;
    }

    public void setAlternativePhoneNumber(String alternativePhoneNumber) {
        this.alternativePhoneNumber = alternativePhoneNumber;
    }

    public String getIpvScreeningStatus() {
        return ipvScreeningStatus;
    }

    public void setIpvScreeningStatus(String ipvScreeningStatus) {
        this.ipvScreeningStatus = ipvScreeningStatus;
    }

    public String getIpvScreeningOutcome() {
        return ipvScreeningOutcome;
    }

    public void setIpvScreeningOutcome(String ipvScreeningOutcome) {
        this.ipvScreeningOutcome = ipvScreeningOutcome;
    }

    public String getNotificationMethod() {
        return notificationMethod;
    }

    public void setNotificationMethod(String notificationMethod) {
        this.notificationMethod = notificationMethod;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
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

    public String getHfrCode() {
        return hfrCode;
    }

    public void setHfrCode(String hfrCode) {
        this.hfrCode = hfrCode;
    }

    public String getCtcNumber() {
        return ctcNumber;
    }

    public void setCtcNumber(String ctcNumber) {
        this.ctcNumber = ctcNumber;
    }
}