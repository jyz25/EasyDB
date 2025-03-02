
1、编译代码
```bash
mvn compile
```

2、执行命令初始化数据库:事务模块
```bash
mvn exec:java -Dexec.mainClass="com.jing.easydb.backend.Launcher" -Dexec.args="-create D:/EasyDB/mydb"
```

3、启动数据库：事务模块
```bash
mvn exec:java -Dexec.mainClass="com.jing.easydb.backend.Launcher" -Dexec.args="-open D:/EasyDB/mydb"
```




