// $Id$
package com.kvs.store;

/**
* Class that serialises the key data.
*/

public class Key {

    private final String key;
    private int offset;
    private final int expiryTime;

    public Key(String key, int offset, int expiryTime) {
        this.key = key;
        this.offset = offset;
        this.expiryTime = expiryTime;
    }

    public String getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public int getExpiryTime() {
        return expiryTime;
    }
    
    public byte[] convertToBytes() {
        int len = key.length();
        byte[] bytes = new byte[8 + 4 + len]; // 4B for offset, 4B for expiryTime, 4B keylen, key
        fillBytes(offset, bytes, 0);
        fillBytes(expiryTime, bytes, 4);
        fillBytes(len, bytes, 8);
        System.arraycopy(key.getBytes(), 0, bytes, 12, len);
        return bytes;
    }
    
    public boolean isExpired() {
        int currTime = (int) (System.currentTimeMillis() / 1000);
        return expiryTime != -1 && currTime > expiryTime;
    }

    private static final void fillBytes(int value, byte[] bytes, int start) {
        bytes[start] = (byte) (value >> 24);
        bytes[start + 1] = (byte) (value >> 16);
        bytes[start + 2] = (byte) (value >> 8);
        bytes[start + 3] = (byte) value;
    }
    
    @Override
    public String toString() {
        return key + " " + offset + " " + expiryTime;
    }

}
