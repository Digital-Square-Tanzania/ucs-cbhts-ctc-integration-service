package com.abt.integration.model;

import java.util.List;

public class VerificationResultsRequest {
    private String hfrCode;
    private List<VerificationResultItem> data;

    public String getHfrCode() {
        return hfrCode;
    }

    public void setHfrCode(String hfrCode) {
        this.hfrCode = hfrCode;
    }

    public List<VerificationResultItem> getData() {
        return data;
    }

    public void setData(List<VerificationResultItem> data) {
        this.data = data;
    }

    public static class VerificationResultItem {
        private String clientCode;
        private String verificationDate;
        private String hivFinalVerificationResultCode;
        private String ctcId;
        private String visitId;

        public String getClientCode() {
            return clientCode;
        }

        public void setClientCode(String clientCode) {
            this.clientCode = clientCode;
        }

        public String getVerificationDate() {
            return verificationDate;
        }

        public void setVerificationDate(String verificationDate) {
            this.verificationDate = verificationDate;
        }

        public String getHivFinalVerificationResultCode() {
            return hivFinalVerificationResultCode;
        }

        public void setHivFinalVerificationResultCode(String hivFinalVerificationResultCode) {
            this.hivFinalVerificationResultCode = hivFinalVerificationResultCode;
        }

        public String getCtcId() {
            return ctcId;
        }

        public void setCtcId(String ctcId) {
            this.ctcId = ctcId;
        }

        public String getVisitId() {
            return visitId;
        }

        public void setVisitId(String visitId) {
            this.visitId = visitId;
        }
    }
}
