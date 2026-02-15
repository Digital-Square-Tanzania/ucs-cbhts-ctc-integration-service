package com.abt.integration.service;

import com.abt.domain.EventRequest;

public interface OpenSrpEventSender {
    String send(EventRequest events, String url, String username, String password);
}
