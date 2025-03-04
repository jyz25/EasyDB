package com.jing.easydb.backend.dm.dataItem;

import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.DataManagerImpl;
import com.jing.easydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {


    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;

    static final int OF_DATA = 3;


    private SubArray raw; // 原始数据

    private byte[] oldRaw;

    private Lock rLock;
    private Lock wLock;

    private DataManagerImpl dm;

    private long uid; // 数据项的唯一标识符

    private Page pg; // 页面对象


    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }


    // 注意 byte[] raw 是引用，所以不会有两份的raw数组
    @Override
    public SubArray data() {
        // 返回 [data] 部分
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    // 在修改数据项之前调用，用于锁定数据项并保存数据
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        //保存原始数据的副本，以便在需要时进行回滚
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    //在需要撤销修改时调用，用于恢复原始数据并解锁数据项
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    // 修改完数据项后，记录日志并解锁数据项
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
