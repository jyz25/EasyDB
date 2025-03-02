package com.jing.easydb.common;

public class Error {
    // common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");


    // tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");



    // launcher
    public static final Exception InvalidMemException = new RuntimeException("Invalid memory!");

}
