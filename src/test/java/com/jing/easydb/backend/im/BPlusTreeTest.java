package com.jing.easydb.backend.im;

import com.jing.easydb.backend.dm.DataManager;
import com.jing.easydb.backend.dm.pageCache.PageCache;
import com.jing.easydb.backend.tm.MockTransactionManager;
import com.jing.easydb.backend.tm.TransactionManager;
import org.junit.Test;


import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

        // 创建一颗 B+ 树的头结点，并返回它的唯一标识的指针
        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for (int i = lim - 1; i >= 0; i--) {
            tree.insert(i, i);
        }

        for (int i = 0; i < lim; i++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
