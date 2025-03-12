package com.jing.easydb.backend.im;


import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.DataManager;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.backend.tm.TransactionManagerImpl;
import com.jing.easydb.backend.im.Node.SearchNextRes;
import com.jing.easydb.backend.im.Node.InsertAndSplitRes;
import com.jing.easydb.backend.im.Node.LeafSearchRangeRes;
import com.jing.easydb.common.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    DataManager dm;
    long bootUid; // 通过它可以得到 bootDataItem
    DataItem bootDataItem; // 通过它可以得到 B+树的 rootUid
    Lock bootLock;

    /**
     * 初始化一颗 BPlusTree
     *
     * @param dm 数据管理器
     * @return bootUid
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        // 包装成数据项 DataItem，并插入到 Page 中，返回唯一标识符
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        // 将rootUid也包装成 DataItem, 也存放到 Page 中,生成 bootUid
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 加载一颗 BPlusTree
     *
     * @param bootUid 启动标识
     * @param dm      数据管理器
     * @return BPlusTree类
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // 根据 bootUid -> 得到rootUid
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新 bootDataItem 中的 rootUid
     *
     * @param left     第一个孩子对应的唯一标识
     * @param right    第二个孩子对应的唯一标识 （它的索引值默认是Integer.MaxValue）
     * @param rightKey 第一个孩子对应的索引值(应该写错了原本)
     * @throws Exception
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // 1、创建根结点对应的二进制数据
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            // 2、将根结点对应的二进制数据持久化，并返回它的全局ID （此处依赖DataManager）
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            // 3、修改数据项之前，获取写锁，并发安全性考虑
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            // 4、修改bootDataItem，将新的rootUid保存到对应为止
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            // 4、修改数据项之后，记录更改日志，并释放写锁
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 递归查找，直到找到 key 所在叶子结点的 nodeUid
     *
     * @param nodeUid Node结点的唯一表示符
     * @param key     需要查找的索引值
     * @return 存在该索引 key 的叶子结点的 nodeUid
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        // 1、根据 nodeUid 加载对应的 Node 对象
        Node node = Node.loadNode(this, nodeUid);
        // 2、判断该 Node 是否是叶子结点 (根据raw的第一个字节判断)
        boolean isLeaf = node.isLeaf();
        // 3、释放这个结点对应的持久化数据(暂时没搞懂为啥)
        node.release();

        if (isLeaf) {
            // 4.1、叶子结点，直接返回nodeUid
            return nodeUid;
        } else {
            // 4.2、非叶子结点，则递归，继续向下遍历查找
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 查找下一个存在 key 的 Node结点，找到直接返回子节点的 nodeUid，
     * 优先在当前结点中找，找到则返回子节点，找不到则从兄弟结点中继续找
     *
     * @param nodeUid Node的唯一标识
     * @param key     索引值
     * @return 下一个存在 key 的 nodeuid
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            // 1、根据nodeUid获取对应的Node对象
            Node node = Node.loadNode(this, nodeUid);
            // 2、查找存在该 key 的下一个结点
            SearchNextRes res = node.searchNext(key);
            // 3、释放当前持有的缓存引用，计数减1
            node.release();
            // 4.1、uid不为0，表示当前结点的子节点存在key，返回子节点的 nodeUid
            if (res.uid != 0) return res.uid;
            // 4.2、uid为0，表示当前结点及其子节点不存在该key，查询其兄弟结点
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 在 BPlusTree 中查找 key 所对应的叶子结点的 nodeUids
     *
     * @param key 索引值
     * @return 包含该索引值的 nodeUid列表
     * @throws Exception
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 同上，查找 [leftKey,rightKey] 对应的 nodeUid列表
     *
     * @param leftKey  索引左值
     * @param rightKey 索引右值
     * @return 叶子结点唯一标识列表
     * @throws Exception
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 1、查找leftKey所在的叶子结点的 nodeUid
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            // 2、加载存放 leftKey 的叶子结点
            Node leaf = Node.loadNode(this, leafUid);
            // 3、查找[leftKey,rightKey]范围内的
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            // 4、回收引用，计数器减1
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    // 向 BPlusTree 中插入新结点 key是结点的索引值，uid是对应
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        // 1、查询nodeUid对应的Node是不是叶子结点
        boolean isLeaf = node.isLeaf();
        node.release(); // 使用完毕后，一定要回归计数

        InsertRes res = null;
        if (isLeaf) {
            // 2.1、是叶子结点，
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 2.2、不是叶子结点
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
