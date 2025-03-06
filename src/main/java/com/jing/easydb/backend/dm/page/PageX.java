package com.jing.easydb.backend.dm.page;


import com.jing.easydb.backend.dm.pageCache.PageCache;
import com.jing.easydb.common.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true); // 将pg的dirty标志设置为true，表示pg的数据已经被修改
        short offset = getFSO(pg.getData()); // 获取pg的空闲空间偏移量
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);// 将raw的数据复制到pg的数据中的offset位置
        setFSO(pg.getData(), (short) (offset + raw.length)); // 更新pg的空闲空间偏移量
        return offset; // 返回插入位置
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     * 将 raw 数据插入到 页面 pg 的 指定位置 offset
     * 如果在插入过程中，空闲区域也被涉及，则更新页的大小信息（页数据的前两个字节）
     *
     * @param pg     具体要插入数据的页
     * @param raw    具体需要插入的数据
     * @param offset 数据需要插入的位置
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // 确定是更新操作，所以不必更新Page头（两个字节表示FSO）
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
