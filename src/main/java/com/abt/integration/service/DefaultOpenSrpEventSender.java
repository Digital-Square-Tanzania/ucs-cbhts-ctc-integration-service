package com.abt.integration.service;

import com.abt.domain.EventRequest;
import com.abt.util.OpenSrpService;

public class DefaultOpenSrpEventSender implements OpenSrpEventSender {
    @Override
    public String send(EventRequest events, String url, String username, String password) {
        return OpenSrpService.sendDataToDestination(events, url, username, password);
    }
}
