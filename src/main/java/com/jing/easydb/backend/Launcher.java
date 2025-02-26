package com.jing.easydb.backend;

import com.jing.easydb.backend.tm.TransactionManager;
import org.apache.commons.cli.*;

public class Launcher {
    public static void main(String[] args) throws ParseException {
        String[] arg = {"-create","D:/EasyDB/mydb"};
        Options options = new Options();
        options.addOption("create", true, "-create DBPath");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, arg);
        if (cmd.hasOption("create")) {
            System.out.println("进入create目录");
            createDB(cmd.getOptionValue("create"));
            return;
        }
    }

    // 用户输入路径，在指定路径下创建数据库
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        tm.close();
    }
}
