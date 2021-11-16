package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.utp.common.model.Message;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class Main {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        String messageListString = "[]";
        List<Message> messageList = mapper.readValue(messageListString, new TypeReference<List<Message>>() {
        });
        System.out.println(messageList.size());
    }
}
