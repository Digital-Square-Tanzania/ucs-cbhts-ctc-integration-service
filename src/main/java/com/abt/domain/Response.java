package com.abt.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Response {

    @JsonProperty("description")
    private String description;

    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("base_entity_id")
    private String baseEntityId;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getBaseEntityId() {
        return baseEntityId;
    }

    public void setBaseEntityId(String baseEntityId) {
        this.baseEntityId = baseEntityId;
    }
}
