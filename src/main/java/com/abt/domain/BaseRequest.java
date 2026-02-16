package com.abt.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseRequest {
    @JsonProperty("team_id")
    private String teamId;

    @JsonProperty("team")
    private String team;

    @JsonProperty("location_id")
    private String locationId;

    @JsonProperty("rec_guid")
    private String recGuid;

    @JsonProperty("provider_id")
    private String providerId;

    public String getRecGuid() {
        return recGuid;
    }

    public void setRecGuid(String recGuid) {
        this.recGuid = recGuid;
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getLocationId() {
        return locationId;
    }

    public void setLocationId(String locationId) {
        this.locationId = locationId;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }
}