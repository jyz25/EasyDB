package com.jing.easydb.backend.im;


import com.jing.easydb.backend.common.SubArray;
import com.jing.easydb.backend.dm.dataItem.DataItem;
import com.jing.easydb.backend.tm.TransactionManagerImpl;
import com.jing.easydb.common.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid] 1 + 2 + 8
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN] (8+8) * N
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0; // 表示该节点是否为叶子节点
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1; // 表示该节点中key的个数
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2; // 表示节点的兄弟节点的UID属性
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8; // 表示节点头部的大小的常量

    static final int BALANCE_NUMBER = 32; // 节点的平衡因子的常量，一个节点最多可以包含32个key
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2); // 节点的大小

    BPlusTree tree;
    DataItem dataItem;  // raw的包装，[ValidFlag] [DataSize] [Data] ValidFlag 1字节，0为合法，1为非法 DataSize 2字节，标识Data的长度
    SubArray raw; // Node结点对应的二进制数据，结构参照类注释
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    /**
     * 将 [son kth][key kth] 向后移动一个身位，将原本的位置空出来
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    /**
     * @param left  根节点的第一颗子树
     * @param right 根节点的最右子树, key默认是Long.MAX_VALUE
     * @param key   根节点的第一颗子树对应的索引key
     * @return 创建根节点对应的二进制数据
     */
    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     * 根据 node 的唯一标识 uid 加载对应的 Node结点 (依赖 DM 层提供服务)
     *
     * @param bTree 对应的 B+树
     * @param uid   对应  Node结点的唯一标识
     * @return 实际的Node对象
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid; // nodeUid，对应 SonN
        long siblingUid; // 同级结点(此处不必要，根据匹配规则，对应的key必然存在当前结点中)
    }

    // 查找存在该key的下一个结点
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            // 1、获取该Node结点的KeyNumber
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                // 2、获取该Node结点中的第 i 个 key
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    // 3.1、key在ik对应的区间里，查找到下一个装有key的Node结点
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 本 Node内所有key都查询了，BPlusTree中压根没有该结点
            res.uid = 0;
            // 从兄弟结点中继续查找
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids; // 存放[leftKey,rightKey]中当前Node包含的部分
        long siblingUid;
    }

    /**
     * 确保调用该方法的一定是叶子结点,返回当前结点能够包含的 key列表，以及超出部分存放的兄弟结点
     *
     * @param leftKey  索引范围的最左索引 key
     * @param rightKey 索引范围的最右索引 key
     * @return LeafSearchRangeRes
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            // 1、找到大于等于且距离 leftKey 最近的key
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            // 记录nodeUids,在其中包括 [leftKey,rightKey] 这些Key
            List<Long> uids = new ArrayList<>();
            // 2、第 K 个结点在当前Node的key列表中，说明leftKey在当前Node里
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    // 这种表明 rightKey不在当前结点中，表示还有其他结点也需要返回
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    // 这种表明 rightKey也在当前结点的keys列表里
                    break;
                }
            }
            long siblingUid = 0;
            if (kth == noKeys) {
                // 存在一部分keys，不在当前结点中的情况
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }


    /**
     * 尝试在当前 Node 中插入 uid 和 key
     *
     * @param uid Node的唯一表示
     * @param key Node对应的索引键
     * @return 插入失败，返回兄弟结点 siblingUid； 插入成功，有分裂情况返回分裂结点，否则返回空数据
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before(); // 修改Node结点对应的实体DataItem，要拿写锁
        try {
            success = insert(uid, key);
            if (!success) {
                // 插入没成功，返回兄弟结点
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 插入成功，判断是否需要继续分裂
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    // 将 uid(实质为soni) 和 key(实质位keyi)插入到结点的指定位置
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        // 遍历当前 Node 的所有 key
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        // 1、key超出当前结点能存放的范围且有兄弟结点，当前结点插入失败
        if (kth == noKeys && getRawSibling(raw) != 0) return false;

        // 2.1、未超出范围，且是叶子结点，将其插入对应的位置
        if (getRawIfLeaf(raw)) {
            // kth 向后移动一个身位
            shiftRawKth(raw, kth);
            // 将key插入到 kth 之前所在的威望值
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            // 2.2、未超出范围，不是叶子结点,同上一样插入位置
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth); // 将key插入kth位置
            shiftRawKth(raw, kth + 1); // 将kth+1的位置挪出来
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    // 判断当前结点空间是否足够，如果不够了需要扩容
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son; // 新结点对应的nodeUid
        res.newKey = getRawKthKey(nodeRaw, 0); // 新结点的第一个key值
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
