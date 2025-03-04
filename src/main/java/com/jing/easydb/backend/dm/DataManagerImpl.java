package com.jing.easydb.backend.dm;


import com.jing.easydb.backend.common.AbstractCache;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.backend.dm.logger.Logger;

public class DataManagerImpl extends AbstractCache<DataItem> {

    Logger logger;


    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected void releaseForCache(DataItem obj) {

    }
}
