package com.jing.easydb.backend.common;


// [start,end)
public class SubArray {
    public byte[] raw; // DataItem所在的Page中的Data(真实数据始终在Page中其他的都是引用)
    public int start;  // DataItem在Page中的Data中的开始位置(包含)
    public int end;    // DataItem在Page中的Data(不包含)

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
