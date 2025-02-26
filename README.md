
编译代码
```bash
mvn compile
```

执行命令初始化数据库:事务系统
```bash
mvn exec:java -Dexec.mainClass="com.jing.easydb.backend.Launcher" -Dexec.args="-create D:/EasyDB/mydb"
```



