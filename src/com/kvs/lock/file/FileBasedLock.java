// $Id$
package com.kvs.lock.file;

import java.io.File;

import com.kvs.lock.Lock;

/**
* Implementation of lock interface which does locking using files.
*/

public class FileBasedLock implements Lock {

    private final File lockFile;

    public FileBasedLock(File dir) {
        String filePath = String.join("/", dir.getAbsolutePath(), "write.lock");
        lockFile = new File(filePath);
    }

    @Override
    public boolean lock() {
        try {
            if (!lockFile.exists() && lockFile.createNewFile()) {
                lockFile.deleteOnExit();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean unlock() {
        return lockFile.delete();
    }

}
