package com.jing.easydb.backend.dm.pageIndex;

import com.jing.easydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 一个区间占用的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    /**
     * 比如 lists[n] = new ArrayList();
     * 即n个区间，其中该lists[n]中的PageInfo的页面大小为[THRESHOLD * n,THRESHOLD * (n+1)) (B)
     */
    private List<PageInfo>[] lists; // 每个List存储有相同空闲空间的PageInfo

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1]; // 每个 List 存放 [x,y) 范围内的数据，所以需要 +1 个桶
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 根据给定的页面编号和空闲空间大小添加一个 PageInfo 对象。
     *
     * @param pgno      页面编号
     * @param freeSpace 页面的空闲空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD; // 计算空闲空间大小对应的区间编号
            lists[number].add(new PageInfo(pgno, freeSpace)); // 在对应的区间列表中添加一个新的 PageInfo 对象
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据给定的空间大小选择一个 PageInfo 对象。
     *
     * @param spaceSize 需要的空间大小
     * @return 一个 PageInfo 对象，其空闲空间大于或等于给定的空间大小。如果没有找到合适的 PageInfo，返回 null。
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD; // 计算需要的空间大小对应的区间编号
            // 此处+1主要为了向上取整
            /*
                1、假需要存储的字节大小为5168，此时计算出来的区间号是25，但是25*204=5100显然是不满足条件的
                2、此时向上取整找到 26，而26*204=5304，是满足插入条件的
                3、此处向上取整没问题，虽然区间25中存储的页面大小在[25*204,26*204)之间，
                spaceSize也在这个区间，但无法确保分配的页面大小是足够的，所以必须向上取整，
                即确保所分配的页面大小必大于spaceSize
             */
            if (number < INTERVALS_NO) number++; // 如果计算出的区间编号小于总的区间数，编号加一
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
