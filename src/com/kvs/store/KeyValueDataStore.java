// $Id$
package com.kvs.store;

import java.io.IOException;

/**
* Structure of a key value data store.
*/

interface KeyValueDataStore {
    
    void add(String key, String value, int ttl) throws IOException;
    
    void delete(String key) throws IOException;
    
    String get(String key) throws IOException;
    
}

    