// $Id$
package com.kvs.test;

import org.json.JSONObject;

import com.kvs.KeyValueStore;

/**
 * A simple test impl.
 */

public class Test {

    public static void main(String[] args) throws Exception {
        KeyValueStore store = new KeyValueStore("/Users/vignesh-2506/kks");
        testAdd(store);
        testDelete(store);
        testDuplicateAdd(store);
        testDeleteNonExistentKey(store);
        testGetNonExistentKey(store);
        testTTL(store);
        System.exit(1);
    }

    private static void testAdd(KeyValueStore store) {
        try {
            JSONObject fJson = new JSONObject();
            fJson.put("aa", "bb");
            store.add("1", fJson);
            JSONObject sJson = new JSONObject();
            sJson.put("cc", "dd");
            sJson.put("ee", "ff");
            store.add("2", sJson);
            JSONObject tJson = new JSONObject();
            tJson.put("gg", "hh");
            tJson.put("ii", "jj");
            tJson.put("kk", "ll");
            store.add("3", tJson);
            fJson.equals(store.get("1"));
            sJson.equals(store.get("2"));
            tJson.equals(store.get("3"));
            store.delete("1");
            store.delete("2");
            store.delete("3");
        } catch (Exception e) {
            System.err.println("testAdd failed.");
            e.printStackTrace();
        }
        System.out.println("testAdd passed.");
    }

    private static void testDelete(KeyValueStore store) {
        try {
            JSONObject fJson = new JSONObject();
            fJson.put("aa", "bb");
            store.add("1", fJson);
            JSONObject sJson = new JSONObject();
            sJson.put("cc", "dd");
            sJson.put("ee", "ff");
            store.add("2", sJson);
            JSONObject tJson = new JSONObject();
            tJson.put("gg", "hh");
            tJson.put("ii", "jj");
            tJson.put("kk", "ll");
            store.add("3", tJson);
            fJson.equals(store.get("1"));
            sJson.equals(store.get("2"));
            tJson.equals(store.get("3"));
            store.delete("1");
            store.delete("2");
            store.delete("3");
            // add will succeed only if the old key got deleted.
            store.add("3", tJson);
            store.delete("3");
        } catch (Exception e) {
            System.err.println("testDelete failed.");
            e.printStackTrace();
        }
        System.out.println("testDelete passed.");
    }

    private static void testDuplicateAdd(KeyValueStore store) {
        try {
            JSONObject fJson = new JSONObject();
            fJson.put("aa", "bb");
            store.add("1", fJson);
            fJson.equals(store.get("1"));
            try {
                store.add("1", fJson);
            } catch (Exception e) {
                System.out.println("testDuplicateAdd passed.");
            }
            store.delete("1");
        } catch (Exception e) {
            System.err.println("testDuplicateAdd failed.");
            e.printStackTrace();
        }
    }

    private static void testDeleteNonExistentKey(KeyValueStore store) {
        try {
            store.delete("1");
        } catch (Exception e) {
            System.out.println("testDeleteNonExistentKey passed.");
            return;
        }
        System.out.println("testDeleteNonExistentKey failed.");
    }
    
    private static void testGetNonExistentKey(KeyValueStore store) {
        try {
            store.get("1");
        } catch (Exception e) {
            System.out.println("testGetNonExistentKey passed.");
            return;
        }
        System.out.println("testGetNonExistentKey failed.");
    }

    private static void testTTL(KeyValueStore store) {
        try {
            JSONObject fJson = new JSONObject();
            fJson.put("aa", "bb");
            store.add("1", fJson, 2);
            fJson.equals(store.get("1"));
            Thread.sleep(5000);
            try {
                store.get("1");
            } catch (Exception e) {
                System.out.println("testTTL passed.");
                return;
            }
        } catch (Exception e) {
            System.out.println("testTTL failed.");
        }
        System.out.println("testTTL failed.");
    }

}
