package com.jing.easydb.backend;

import com.jing.easydb.backend.tm.TransactionManager;
import com.jing.easydb.backend.utils.Panic;
import com.jing.easydb.common.Error;
import org.apache.commons.cli.*;

public class Launcher {

    // 系统占用的总内存空间64M
    public static final long DEFALUT_MEM = (1 << 20) * 64;

    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
//        String[] arg = {"-create","D:/EasyDB/mydb"};
        Options options = new Options();
        options.addOption("create", true, "-create DBPath");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }

        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
    }

    // 用户输入路径，在指定路径下创建数据库
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        tm.close();
    }

    // 用户传入数据库的路径以及内存大小参数，用来启动数据库
    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        System.out.println("================");
    }

    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
