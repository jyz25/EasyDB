package com.jing.easydb.backend.utils;

public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1); // 终止 JVM进程
    }
}
