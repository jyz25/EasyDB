package com.jing.easydb.backend.utils;

public class Types {
    // 通过pgno和offset来生成addressToUid
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long) pgno;
        long u1 = (long) offset;
        return u0 << 32 | u1;
    }
}
