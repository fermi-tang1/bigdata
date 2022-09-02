# Hive installation(Mac)
1. ```
   brew install hive
   ```
2. ```
   open ~/.bash_profile
   export HIVE_HOME=/usr/local/Cellar/hive/3.1.3
   export PATH=$HIVE_HOME/bin
   source ~/.bash_profile
   ```
3. ```
   cd /usr/local/Cellar/hive/3.1.3/libexec/conf
   vi hive-site.xml
   
   <configuration>
    <property>
    <name>hive.metastore.local</name>
    <value>true</value>
    </property>
    <property>
    <!--mysql local-->
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/metastore</value>
        </property>
         <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>
    <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>root</value>
    </property>
    <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>123456</value>
    </property>
    <property>
        <name>hive.exec.local.scratchdir</name>
        <value>/Users/ftang/Downloads/hive</value>
    </property>

    <property>
        <name>hive.downloaded.resources.dir</name>
            <value>/Users/ftang/Downloads/hive</value>
    </property>

    <!--local hdfs url-->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://localhost:9000/user/hive/warehouse</value>
    </property>

    <property>
        <name>hive.server2.logging.operation.log.location</name>
        <value>/Users/ftang/Downloads/hive</value>
    </property>
    </configuration>
    ```

4. ```
   init 
   cd /usr/local/Cellar/hive/3.1.3/libexec/lib
   curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar
    ```
5. ```
    create meta data db in mysql:
    create database metastore;
    ```
6. ```
   init database:
   schematool -dbType mysql -initSchema
   ```


###Create tables

```
1. Starting hiveserver2 (use root user)
2. beeline
3. beeline> !connect jdbc:hive2://localhost:10000/metastore
Connecting to jdbc:hive2://localhost:10000/metastore
Enter username for jdbc:hive2://localhost:10000/metastore: root
Enter password for jdbc:hive2://localhost:10000/metastore: ******
4. create table t_user (user_id int, sex string, age int, occupation string, zipcode string) row format delimited fields terminated by '::';
   create table t_movie (movie_id int, movie_name string, movie_type string) row format delimited fields terminated by '::';
   create table t_rating (user_id int,movie_id int, rate int, times string) row format delimited fields terminated by '::';
````
#### notes
```
1. update hive-site.xml(hive)
   <property>
            <!-- hiveserver2用户名 -->
            <name>beeline.hs2.connection.user</name>
            <value>root</value>
    </property>

    <property>
            <!-- hiveserver2密码 -->
            <name>beeline.hs2.connection.password</name>
            <value>123456</value>
    </property>

    <property>
            <!-- hiveserver2端口 -->
            <name>beeline.hs2.connection.hosts</name>
            <value>localhost:10000</value>
    </property>
    
2. update core-site.xml(hadoop)

    <property>
      <name>hadoop.proxyuser.hadoop.hosts</name>
      <value>*</value>
    </property>
    <property>
      <name>hadoop.proxyuser.hadoop.groups</name>
      <value>*</value>
    </property>

3. restart hive and hadoop server
```
###Import data to hdfs
```
1. hive (use root user)
2. load data local inpath '/Users/ftang/Downloads/users.dat' into table metastore.t_user;
   load data local inpath '/Users/ftang/Downloads/movies.dat' into table metastore.t_movie;
   load data local inpath '/Users/ftang/Downloads/ratings.dat' into table metastore.t_rating;
```

##Notes
FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
add this in mapred-site.xml(hadoop conf)
```
  <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>8192</value>
    </property>
      <property>
        <name>mapreduce.map.memory.mb</name>
        <value>8192</value>
    </property>
```


## InvertedIndex
　run InvertedIndex.main, output to /bigdata/source/output/

## Dist Copy
1. use local configure, run SparkDistcp.main
2. mvn clean package
use spark-submit: spark-submit --master local --class org.example.bigdata.spark.SparkDistcp /Users/ftang/Desktop/bigdata-1.0-SNAPSHOT.jar -ifp /user/root/input -ofp output -i TRUE -m 6
   


## Homework - Week 8
1. Task 2: Run DataFrameTest to test the spark sql Optimization rules.
2. Task 3: 自定义了一个规则，当查询结果小于1时，不需要应用排序。
   2.1 mvn clean package
   2.2 ./spark-sql --jars /Users/ftang/WorkSpace/bigdata/target/bigdata-1.0-SNAPSHOT.jar  --conf spark.sql.extensions=MySparkSessionExtension
   2.3 spark-sql> set spark.sql.planChangeLog.level=WARN;
   2.4 spark-sql> create temporary view test as select * from values
                    ("A", 1),
                    ("B", 2),
                    ("C", 3),
                    ("D", 4)
                    as test(name, value);
   2.5 SELECT * FROM test where value < 2 order by value; 
   ```` 查看log可以看到：The maximum number of rows are less than 1, no need to sort!!!22/09/02 17:36:05 WARN PlanChangeLogger:
    === Applying Rule CustomRule ===
   !Sort [value#13 ASC NULLS FIRST], true   LocalRelation [name#12, value#13]
   !+- LocalRelation [name#12, value#13]
   ```
   2.6 SELECT * FROM test where value < 4 order by value;  // 查看不到过滤的Log

