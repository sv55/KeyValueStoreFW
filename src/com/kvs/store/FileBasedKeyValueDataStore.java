// $Id$
package com.kvs.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A file based data store.
 * 
 * Here there are two files - keys & values.
 * 
 * keys file contain the key's name, offset of it's value in the "values" file
 * and the expiry time.
 * 
 * values file contain the actual value.
 * 
 * Only the keys data is kept in memory. For every get, the offset of the values
 * file is obtained and the value is read.
 */

class FileBasedKeyValueDataStore implements KeyValueDataStore {

    private static final long MAX_LEN = 1024 * 1024 * 1024;
    private static final ConcurrentHashMap<String, KeyValueDataStore> INSTANCE_MAP = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Key> keyMap = new ConcurrentHashMap<>();
    private final String dir;
    private File keyFile;
    private File valueFile;
    private final AtomicBoolean isDeletePerformed = new AtomicBoolean(false);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /*
     * As all the data is written in an append only manner. We have to
     * periodically remove deleted items from the files. This compaction thread
     * rewrites the file to remove the deleted items.
     */
    private final TimerTask compactionTask = new TimerTask() {

        @Override
        public void run() {
            try {
                compact();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    static KeyValueDataStore getInstance(String dir) throws IOException {
        if (!INSTANCE_MAP.containsKey(dir)) {
            INSTANCE_MAP.put(dir, new FileBasedKeyValueDataStore(dir));
        }
        return INSTANCE_MAP.get(dir);
    }

    private FileBasedKeyValueDataStore(String dir) throws IOException {
        this.dir = dir;
        this.keyFile = new File(dir + File.separator + "keys");
        this.valueFile = new File(dir + File.separator + "values");
        loadExistingData();
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(compactionTask, TimeUnit.MINUTES.toMillis(2), TimeUnit.MINUTES.toMillis(2));
    }

    /*
     * Loads all the existing unexpired keys into the memory.
     */
    private void loadExistingData() throws IOException {
        if (keyFile.exists()) {
            FileInputStream in = new FileInputStream(keyFile);
            try {
                byte[] bytes = new byte[4];
                while (in.read(bytes) != -1) {
                    int offset = getAsInt(bytes);
                    in.read(bytes);
                    int ttl = getAsInt(bytes);
                    in.read(bytes);
                    int len = getAsInt(bytes);
                    byte[] keyBytes = new byte[len];
                    in.read(keyBytes);
                    String keyStr = new String(keyBytes);
                    Key key = new Key(keyStr, offset, ttl);
                    if (!key.isExpired()) {
                        keyMap.put(keyStr, key);
                    }
                }
            } finally {
                in.close();
            }
        } else {
            keyFile.createNewFile();
            valueFile.createNewFile();
        }
        System.out.println(keyMap);
    }

    /*
     * Adds a value to the store if it does not exist.
     */
    @Override
    public void add(String key, String value, int ttl) throws IOException {
        lock.writeLock().lock();
        try {
            // check to ensure that file size is not greater than 1GB.
            long totalLen = keyFile.length() + valueFile.length();
            if (totalLen > MAX_LEN) {
                throw new RuntimeException("Total data size has crossed 1GB");
            }
            if (keyMap.containsKey(key) && !keyMap.get(key).isExpired()) {
                throw new RuntimeException("Key already exists.");
            } else {
                // first persist the value to get the offset
                int offset = persistValue(valueFile, value);
                // set the expiry time based on the TTL
                int expiryTime = -1;
                if (ttl > 0) {
                    int currTime = (int) (System.currentTimeMillis() / 1000);
                    expiryTime = currTime + ttl;
                }
                Key sKey = new Key(key, offset, expiryTime);
                keyMap.put(key, sKey);
                // persist the key to the file.
                persistKey(keyFile, sKey);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.writeLock().lock();
        try {
            if (!keyMap.containsKey(key)) {
                throw new RuntimeException("Key does not exist.");
            }
            keyMap.remove(key);
            // set this as true for compaction to run.
            isDeletePerformed.set(true);
            // since we don't know what keys have been deleted, persist all keys
            persistAllKeys(keyFile, keyMap);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) throws IOException {
        lock.readLock().lock();
        try {
            if (!keyMap.containsKey(key)) {
                throw new RuntimeException("Key does not exist.");
            }
            Key sKey = keyMap.get(key);
            if (sKey.isExpired()) {
                throw new RuntimeException("Key does not exist.");
            } else {
                return getValue(sKey.getOffset());
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private synchronized void persistKey(File file, Key key) throws IOException {
        FileOutputStream out = new FileOutputStream(file, true);
        try {
            out.write(key.convertToBytes());
        } finally {
            out.close();
        }
    }

    private synchronized void persistAllKeys(File file, Map<String, Key> map) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        try {
            for (Key key : map.values()) {
                out.write(key.convertToBytes());
            }
        } finally {
            out.close();
        }
    }

    private synchronized int persistValue(File file, String value) throws IOException {
        FileOutputStream out = new FileOutputStream(file, true);
        try {
            long currPos = out.getChannel().position();
            out.write(getBytes(value.length()));
            out.write(value.getBytes());
            return (int) currPos;
        } finally {
            out.close();
        }
    }

    private String getValue(int offset) throws IOException {
        FileInputStream in = new FileInputStream(valueFile);
        try {
            byte[] bytes = new byte[4];
            in.getChannel().position(offset);
            in.read(bytes);
            int len = getAsInt(bytes);
            byte[] valueBytes = new byte[len];
            in.read(valueBytes);
            return new String(valueBytes);
        } finally {
            in.close();
        }
    }

    /*
     * Gets all the existing keys. For all unexpired keys, the value is written
     * into the new file. After all values and keys are written to the new ones,
     * during the post compact ops - files are replaced.
     * 
     * All CD ops will wait during compaction. All get ops will wait during post
     * compaction ops.
     */
    private void compact() throws IOException {
        lock.writeLock().lock();
        try {
            if (isCompactionNeeded()) {
                isDeletePerformed.set(false);
                Set<String> keys = keyMap.keySet();
                Map<String, Key> newKeys = new HashMap<>();
                File newValuesFile = new File(dir + File.separator + "newvalues");
                newValuesFile.createNewFile();
                if (newValuesFile.exists()) {
                    for (String key : keys) {
                        Key sKey = keyMap.get(key);
                        if (sKey != null && !sKey.isExpired()) {
                            int offset = persistValue(newValuesFile, get(key));
                            Key newKey = new Key(key, offset, sKey.getExpiryTime());
                            newKeys.put(key, newKey);
                        }
                    }
                    File newKeysFile = new File(dir + File.separator + "newkeys");
                    newKeysFile.createNewFile();
                    persistAllKeys(newKeysFile, newKeys);
                    int oldSize = keyMap.size();
                    doPostCompactOps(newKeysFile, newValuesFile);
                    System.out.println("Compaction done. " + (oldSize - keyMap.size()) + " key(s) were removed.");
                }
            } else {
                System.err.println("Compaction not needed since there are no expired/ deleted keys.");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /*
     * Replaces the old files with new ones and reopens them.
     */
    private void doPostCompactOps(File newKeysFile, File newValuesFile) throws IOException {
        lock.readLock().lock();
        try {
            keyFile.delete();
            valueFile.delete();
            newKeysFile.renameTo(keyFile);
            newValuesFile.renameTo(valueFile);
            this.keyFile = new File(dir + File.separator + "keys");
            this.valueFile = new File(dir + File.separator + "values");
            keyMap.clear();
            loadExistingData();
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isCompactionNeeded() {
        // either delete has to done or any of the keys should have expired.
        return isDeletePerformed.get() || keyMap.values().stream().anyMatch(Key::isExpired);
    }

    private static final byte[] getBytes(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >> 24);
        bytes[1] = (byte) (value >> 16);
        bytes[2] = (byte) (value >> 8);
        bytes[3] = (byte) value;
        return bytes;
    }

    private static final int getAsInt(byte[] bytes) {
        return (bytes[0] & 0xFF) << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }

}