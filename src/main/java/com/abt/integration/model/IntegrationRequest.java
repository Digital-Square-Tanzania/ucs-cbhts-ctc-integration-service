package com.abt.integration.model;

public class IntegrationRequest {
    private String hfrCode;
    private Long startDate;
    private Long endDate;
    private Integer pageIndex;
    private Integer pageSize;

    public String getHfrCode() {
        return hfrCode;
    }

    public void setHfrCode(String hfrCode) {
        this.hfrCode = hfrCode;
    }

    public Long getStartDate() {
        return startDate;
    }

    public void setStartDate(Long startDate) {
        this.startDate = startDate;
    }

    public Long getEndDate() {
        return endDate;
    }

    public void setEndDate(Long endDate) {
        this.endDate = endDate;
    }

    public Integer getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(Integer pageIndex) {
        this.pageIndex = pageIndex;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}
