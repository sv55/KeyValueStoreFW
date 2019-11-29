// $Id$
package com.kvs.store;

import java.io.IOException;

/**
* Class that tell which impl of data store is to be used.
*/

public class KeyValueDataStoreManager {

    public static KeyValueDataStore getDataStore(String dir) throws IOException {
        return FileBasedKeyValueDataStore.getInstance(dir);
    }
}
