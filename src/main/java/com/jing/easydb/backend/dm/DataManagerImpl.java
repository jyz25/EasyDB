package com.jing.easydb.backend.dm;

import com.jing.easydb.backend.common.AbstractCache;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.backend.dm.dataItem.DataItemImpl;
import com.jing.easydb.backend.dm.logger.Logger;
import com.jing.easydb.backend.dm.page.Page;
import com.jing.easydb.backend.dm.page.PageOne;
import com.jing.easydb.backend.dm.page.PageX;
import com.jing.easydb.backend.dm.pageCache.PageCache;
import com.jing.easydb.backend.dm.pageIndex.PageIndex;
import com.jing.easydb.backend.dm.pageIndex.PageInfo;
import com.jing.easydb.backend.tm.TransactionManager;
import com.jing.easydb.backend.utils.Panic;
import com.jing.easydb.backend.utils.Types;
import com.jing.easydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    // 从缓存中拿取数据
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // 向缓存中插入数据
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将输入的数据包装成DataItem的原始格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 如果数据项的大小超过了页面的最大空闲空间，抛出异常
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 初始化一个页面信息对象
        PageInfo pi = null;
        // 尝试5次找到一个可以容纳新数据项的页面
        for (int i = 0; i < 5; i++) {
            // 从页面索引中选择一个可以容纳新数据项的页面
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 如果还是没有找到合适的页面，抛出异常
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        // 初始化一个页面对象
        Page pg = null;
        // 初始化空闲空间大小为0
        int freeSpace = 0;
        try {
            // 获取页面信息对象中的页面
            pg = pc.getPage(pi.pgno);
            // 生成插入日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            // 将日志写入日志文件
            logger.log(log);

            // 在页面中插入新的数据项，并获取其在页面中的偏移量
            short offset = PageX.insert(pg, raw);

            // 释放页面
            pg.release();
            // 返回新插入的数据项的唯一标识符
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 根据uid，从缓存中定位提取数据项，用于缓存中没有数据项时，加载数据到缓存中
     *
     * @param uid 数据项的唯一标识符：由页号和页内地址拼接而成
     * @return 数据项
     * @throws Exception 异常
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 计算页内偏移量
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        // 计算页号
        int pgno = (int) (uid & ((1L << 32) - 1));
        // 从页缓存中拿到页面对象,如果页缓存不在内存中，则加载页面到缓存中
        Page pg = pc.getPage(pgno);
        // 根据偏移量从页面中提取数据项
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * 传入数据项，从缓存中释放该数据项对应的页面
     *
     * @param di 数据项
     */
    @Override
    protected void releaseForCache(DataItem di) {
        // 通过数据项，定位到数据项对应的页面，释放该页面
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    // 刚创建.db文件时，文件大小是0，没有页面存储
    // 初始化第一页，有妙用
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化PageIndex
     * 填充 PageIndex。
     * 遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中。
     */
    void fillPageIndex() {
        // 获取当前页面数量
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) { // 从第二页开始，对每一页进行处理
            Page pg = null;
            try {
                pg = pc.getPage(i); // 尝试获取页面
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 将页面编号和页面的空闲空间大小添加到 PageIndex 中
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

}
