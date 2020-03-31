package ru.gnkoshelev.kontur.intern.redis.map;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

/**
 * @author Gregory Koshelev
 */
public class RedisMapTest {
    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new RedisMap();

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));

        map1.clear();
        Map<String, String> res = Map.of("1", "a", "2", "b");
        map1.putAll(res);
        Assert.assertEquals(map1.get("1"), "a");
        Assert.assertEquals(map1.get("2"), "b");
    }

    @Test
    public void baseTest2() {
        RedisMap redisMap = new RedisMap();
        Map<String, String> redisMap2 = new RedisMap();
        Map<String, String> redisMap3 = new RedisMap(redisMap.getHashUUID());
        Map<String, String> hashMap = new HashMap<>();

        redisMap.put("1", "2");
        redisMap2.put("1", "2");

        Assert.assertEquals(redisMap, redisMap);

        Assert.assertEquals(redisMap, redisMap2);
        Assert.assertEquals(redisMap2, redisMap);

        Assert.assertEquals(redisMap, redisMap2);
        Assert.assertEquals(redisMap2, redisMap3);
        Assert.assertEquals(redisMap3, redisMap);

        redisMap.remove("1");
        Assert.assertNotEquals(redisMap, redisMap2);

        hashMap.put("a", "a");
        hashMap.put("b", "b");
        hashMap.put("c", "c");
        hashMap.put("d", "d");
        redisMap.putAll(hashMap);
        Set<Map.Entry<String, String>> entries = redisMap.entrySet();
        entries.forEach(e -> Assert.assertEquals(hashMap.get(e.getKey()), e.getValue()));

        Assert.assertEquals("a", redisMap.remove("a"));
        Assert.assertNull(redisMap.remove("5"));

        Assert.assertNull(redisMap.put("a", "a"));
        Assert.assertEquals("a", redisMap.put("a", "b"));
        Assert.assertFalse(redisMap.isEmpty());


    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionTest() {
        Map<String, String> map1 = new RedisMap();
        map1.put(null, "12");
    }

    @Test
    public void cleanTest() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        RedisMap map = new RedisMap(uuid);
        Assert.assertEquals(uuid, map.getHashUUID());
        map.put("1", "2");
        map = null;
        System.gc();
        sleep(100L);
        RedisMap testClean = new RedisMap(uuid);
        Assert.assertEquals(0, testClean.size());

    }

    @Test
    public void concurrentTest() throws ExecutionException, InterruptedException {
        String uuid = UUID.randomUUID().toString();
        RedisMap map = new RedisMap(uuid);
        RedisMap map2 = new RedisMap(uuid);
        map.put("1", "2");
        Assert.assertEquals("2", map2.get("1"));
        Assert.assertEquals(map, map2);

        map2.clear();
        Assert.assertTrue(map.isEmpty());
        Assert.assertTrue(map2.isEmpty());
        Assert.assertEquals(0, map.size());

        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> map.putAll(Map.of("3", "4"))),
                CompletableFuture.runAsync(() -> map2.putAll(Map.of("4", "5"))),
                CompletableFuture.runAsync(() -> map.putAll(Map.of("1", "2"))),
                CompletableFuture.runAsync(() -> map2.putAll(Map.of("3", "4")))
        ).get();
        Assert.assertEquals("5", map.get("4"));

        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> map.put("1", "2")),
                CompletableFuture.runAsync(() -> map2.remove("1"))
        ).get();
        Assert.assertNull(map.get("1"));

        MultithreadedStressTester stressTester = new MultithreadedStressTester(6, 25000);
        map.clear();
        AtomicInteger i = new AtomicInteger();
        stressTester.stress(() -> {
            i.addAndGet(1);
            map.put(String.valueOf(i.get()), "test");
        });

        stressTester.shutdown();
        Assert.assertEquals(150000, map.size());

        stressTester = new MultithreadedStressTester(6, 25000);
        AtomicInteger j = new AtomicInteger();
        stressTester.stress(() -> {
            j.addAndGet(1);
            map.remove(String.valueOf(j.get()));
        });
        stressTester.shutdown();
        Assert.assertEquals(0, map.size());
    }

    @Test
    public void finalizeTest() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        RedisMap map = new RedisMap(uuid);
        map.put("1", "2");
        RedisMap map3 = new RedisMap();
        RedisMap testClean = new RedisMap(uuid);
        map = null;
        System.gc();
        sleep(100L);
        Assert.assertEquals(1, testClean.size());
        Assert.assertEquals(0, map3.size());

        testClean = null;
        System.gc();
        sleep(100L);

        RedisMap map2 = new RedisMap(uuid);
        Assert.assertEquals(0, map2.size());
    }
}
