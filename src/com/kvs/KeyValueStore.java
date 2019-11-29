// $Id$
package com.kvs;

import java.io.IOException;

import org.json.JSONObject;

import com.kvs.store.StoreDirectory;

/**
 * This is the entry class. It contains the store directory.
 * This class has all the CRD functions that the client can use.
 */

public class KeyValueStore {

    private final StoreDirectory storeDir;

    public KeyValueStore(String dir) throws IOException {
        storeDir = new StoreDirectory(dir);
    }

    public void add(String key, JSONObject value) throws IOException {
        add(key, value, -1);
    }

    public void add(String key, JSONObject value, int ttl) throws IOException {
        if (key.length() > 32) {
            throw new RuntimeException("Key size greater than 32 chars.");
        }
        String val = value.toString();
        if (val.length() > (16 * 1024)) {
            throw new RuntimeException("Value size greater than 16KB.");
        }
        storeDir.add(key, val, ttl);
    }

    public void delete(String key) throws IOException {
        storeDir.delete(key);
    }

    public JSONObject get(String key) throws Exception {
        return new JSONObject(storeDir.get(key));
    }

}
