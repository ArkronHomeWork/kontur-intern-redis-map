package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Gregory Koshelev
 */
public class RedisMap implements Map<String, String> {

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Jedis jedis = new Jedis();
    private String hashUUID;

    private String lockTable;

    public RedisMap() {
        this(UUID.randomUUID().toString());
    }

    public RedisMap(String hashUUID) {
        checkNullObject(hashUUID);
        this.hashUUID = hashUUID;
        lockTable = "lock#" + hashUUID;
        lock.writeLock().lock();
        try {
            jedis.lpush(lockTable, "ok");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String getHashUUID() {
        return hashUUID;
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return jedis.hgetAll(hashUUID).size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNullObject(key);
        lock.readLock().lock();
        try {
            return jedis.hget(hashUUID, (String) key) != null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        checkNullObject(value);
        lock.readLock().lock();
        try {
            return jedis.hgetAll(hashUUID).containsValue(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String get(Object key) {
        checkNullObject(key);
        lock.readLock().lock();
        try {
            return jedis.hget(hashUUID, (String) key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String put(String key, String value) {
        checkNullObject(key);
        checkNullObject(value);
        lock.writeLock().lock();
        try {
            String val = jedis.hget(hashUUID, key);
            jedis.hset(hashUUID, key, value);
            return val;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String remove(Object key) {
        checkNullObject(key);
        lock.writeLock().lock();
        try {
            String val = jedis.hget(hashUUID, (String) key);
            if (val != null) {
                jedis.hdel(hashUUID, (String) key);
            }
            return val;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        Map<String, String> data = new HashMap<>();
        m.forEach((k, v) -> {
            checkNullObject(k);
            checkNullObject(v);
            data.put(k, v);
        });
        lock.writeLock().lock();
        try {
            jedis.hset(hashUUID, data);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            jedis.hkeys(hashUUID).forEach(key ->
                    jedis.hdel(hashUUID, key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<String> keySet() {
        lock.readLock().lock();
        try {
            return jedis.hkeys(hashUUID);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<String> values() {
        lock.readLock().lock();
        try {
            return jedis.hvals(hashUUID);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        lock.readLock().lock();
        try {
            return jedis.hgetAll(hashUUID).entrySet();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    private void checkNullObject(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Argument can't be null");
        }
    }

    private void close() {
        lock.writeLock().lock();
        try {
            jedis.lpop(lockTable);
        } finally {
            lock.writeLock().unlock();
        }
        AtomicLong len = new AtomicLong();
        lock.readLock().lock();
        try {
            len.compareAndSet(0, jedis.llen(lockTable));
        } finally {
            lock.readLock().unlock();
        }
        if (len.get() == 0) {
            clear();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof RedisMap) {
            return this.hashUUID.equals(((RedisMap) obj).hashUUID) || this.entrySet().equals(((RedisMap) obj).entrySet());
        }
        return false;
    }
}
