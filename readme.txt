Apps used in this project:
1. Apache Hadoop v3.2.1
2. Apache Hive v3.1.2
3. Apache Flume v1.6.0

Before running the app make sure that:
1. Apache Hive is configured.
2. Apache Flume is configured.
3. Apache Hive server 2 configured and running.
4. Hadoop services are configured and running.
5. There is a "twitter.conf" file in "/path/to/flume/conf" folder.
6. A table named "dictionary" is created in hive with AFINN-111.txt loaded into the "dictionary" table.
7. Apache Flume is storing twitter data in hdfs in the "/user/Hadoop/twitter-data" folder.
8. Apache Flume "twitter.conf" path in the Java application may need to be modified.
9. JavaFX library is installed(it is recommended to use a Java version that already has the JavaFX module).

Troubleshooting:
1. Check the Hive and Hadoop configuration xml files with the ones including in the Project Files folder in this project.
2. Check if the correct Apache flume "twitter.conf" is set in the Java application.
3. Check to see if the run time configuration contains the following:
  --module-path /path/to/javafx-sdk-x.x.x/lib --add-modules javafx.controls,javafx.fxml //needed for standalone JavaFX library
  -Xms1024M
  -Xmx4096M
  
Note:
There are some functionalities present in the project that is intended for developmental purposes and may not work as intended or may not work at all.
You may have to modify the java code to properly run this application. 
