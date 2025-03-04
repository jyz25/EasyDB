package com.jing.easydb.backend.common;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private Lock lock;
    private int count = 0;                              // 缓存中元素的个数


    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源被驱逐时的写回行为
     * 即释放缓存时，需要把脏页面写回
     */
    protected abstract void releaseForCache(T obj);

}
