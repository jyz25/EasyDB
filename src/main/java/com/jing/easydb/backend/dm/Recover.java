package com.jing.easydb.backend.dm;

import com.google.common.primitives.Bytes;
import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.common.Parser;

import java.util.Arrays;

public class Recover {

    private static final byte LOG_TYPE_UPDATE = 1;


    /**
     * 创建一个更新日志
     *
     * @param xid 事务ID
     * @param di  DataItem对象
     * @return 更新日志: 包含日志类型、事务ID、DataItem的唯一标识符、旧原始数据和新原始数据
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }
}
