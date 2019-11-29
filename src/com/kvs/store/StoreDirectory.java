// $Id$
package com.kvs.store;

import java.io.File;
import java.io.IOException;

import com.kvs.lock.Lock;
import com.kvs.lock.file.FileBasedLock;

/**
* The key value store directory class.
*/

public class StoreDirectory {

    private final Lock lock;
    private final KeyValueDataStore dataStore;

    public StoreDirectory(String dir) throws IOException {
        File directory = new File(dir);
        if (directory.isDirectory()) {
            lock = new FileBasedLock(directory);
            boolean isLocked = lock.lock();
            if (!isLocked) {
                throw new RuntimeException("Lock cannot be obtained for the given key-value store");
            } else {
                System.out.println("Key-value store initialized at " + dir);
            }
        } else {
            throw new RuntimeException(dir + " is not a directory.");
        }
        this.dataStore = KeyValueDataStoreManager.getDataStore(dir);
    }

    public void add(String key, String value, int ttl) throws IOException {
        dataStore.add(key, value, ttl);
    }

    public void delete(String key) throws IOException {
        dataStore.delete(key);
    }

    public String get(String key) throws IOException {
        return dataStore.get(key);
    }
}
