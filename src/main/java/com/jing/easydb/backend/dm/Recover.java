package com.jing.easydb.backend.dm;

import com.google.common.primitives.Bytes;
import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.backend.dm.logger.Logger;
import com.jing.easydb.backend.dm.page.Page;
import com.jing.easydb.backend.dm.page.PageX;
import com.jing.easydb.backend.dm.pageCache.PageCache;
import com.jing.easydb.backend.tm.TransactionManager;
import com.jing.easydb.backend.utils.Panic;
import com.jing.easydb.common.Parser;


import java.util.*;
import java.util.Map.Entry;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    // updateLog:
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]

    // insertLog:
    // [LogType] [XID] [Pgno] [Offset] [Raw]

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 重置日志文件的读取位置到开始
        lg.rewind();
        // 循环读取日志文件中的所有日志记录
        while (true) {
            // 读取下一条日志记录
            byte[] log = lg.next();
            // 如果读取到的日志记录为空，表示已经读取到日志文件的末尾，跳出循环
            if (log == null) break;
            // 判断日志记录的类型
            if (isInsertLog(log)) {
                // 如果是插入日志，解析日志记录，获取插入日志信息
                InsertLogInfo li = parseInsertLog(log);
                // 获取事务ID
                long xid = li.xid;
                // 如果当前事务已经提交，进行重做操作
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 如果是更新日志，解析日志记录，获取更新日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                // 获取事务ID
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * 创建一个更新日志。
     *
     * @param xid 事务ID
     * @param di  DataItem对象
     * @return 更新日志，包含日志类型、事务ID、DataItem的唯一标识符、旧原始数据和新原始数据
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE}; // 创建一个表示日志类型的字节数组，并设置其值为LOG_TYPE_UPDATE
        byte[] xidRaw = Parser.long2Byte(xid); // 将事务ID转换为字节数组
        byte[] uidRaw = Parser.long2Byte(di.getUid()); // 将DataItem对象的唯一标识符转换为字节数组
        byte[] oldRaw = di.getOldRaw(); // 获取DataItem对象的旧原始数据
        SubArray raw = di.getRaw(); // 获取DataItem对象的新原始数据
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end); // 将新原始数据转换为字节数组
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw); // 将所有字节数组连接在一起，形成一个完整的更新日志，并返回这个日志
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno; // 用于存储页面编号
        short offset; // 用于存储偏移量
        byte[] raw; // 用于存储原始数据
        if (flag == REDO) {
            // 如果是重做操作，解析日志记录，获取更新日志信息，主要获取新数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            // 如果是撤销操作，解析日志记录，获取更新日志信息，主要获取旧数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null; // 用于存储获取到的页面
        try {
            // 尝试从页面缓存中获取指定页码的页面
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 在指定的页面和偏移量处插入解析出的数据, 数据页缓存讲解了该方法
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            // 无论是否发生异常，都要释放页面
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    // 定义一个静态方法，用于创建插入日志
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        // 创建一个表示日志类型的字节数组，并设置其值为LOG_TYPE_INSERT
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        // 将事务ID转换为字节数组
        byte[] xidRaw = Parser.long2Byte(xid);
        // 将页面编号转换为字节数组
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        // 获取页面的第一个空闲空间的偏移量，并将其转换为字节数组
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        // 将所有字节数组连接在一起，形成一个完整的插入日志，并返回这个日志
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        // 解析日志记录，获取插入日志信息
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            // 根据页码从页面缓存中获取页面，即AbstractCache.get()方法
            pg = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 如果标志位为UNDO，将数据项设置为无效
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 在指定的页面和偏移量处插入数据[REDO 核心执行代码]
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            // 无论是否发生异常，都要释放页面,即AbstractCache.release() 方法
            pg.release();
        }
    }
}
