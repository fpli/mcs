package com.ebay.app.raptor.chocolate.controller;

import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClientV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("mkt/cb")
public class CouchbaseController {
    @Autowired
    CouchbaseClientV2 couchbaseClientV2;

    @PostMapping("{key}")
    public void putValue(String value, @PathVariable("key") String key) {
        couchbaseClientV2.put(key, value, 0);
    }

    @GetMapping("{key}")
    public String getValue(@PathVariable("key") String key) {
        return couchbaseClientV2.get(key);
    }
}
