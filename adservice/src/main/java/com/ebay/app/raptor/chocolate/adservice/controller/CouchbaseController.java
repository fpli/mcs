package com.ebay.app.raptor.chocolate.adservice.controller;

import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("mkt/cb")
public class CouchbaseController {

    @PostMapping("{key}")
    public void putValue(String value, @PathVariable("key") String key) {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        instance.put(key, value, 0);
    }

    @GetMapping("{key}")
    public String getValue(@PathVariable("key") String key) {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        return instance.get(key);
    }
}
