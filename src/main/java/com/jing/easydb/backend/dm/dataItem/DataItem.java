package com.jing.easydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.DataManagerImpl;
import com.jing.easydb.backend.dm.page.Page;
import com.jing.easydb.backend.utils.Types;
import com.jing.easydb.common.Parser;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    /**
     * 返回一个完整的 DataItem 结构数据
     * dataItem 结构如下：
     * [ValidFlag] [DataSize] [Data]
     * ValidFlag 1字节，0为合法 1为非法
     * DataSize  2字节，标识Data的长度
     *
     * @param raw 原始数据
     * @return 包装后 DataItem对应的数据
     */

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1]; // 初始为不合法数据
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析出DataItem
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);

    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }


}
