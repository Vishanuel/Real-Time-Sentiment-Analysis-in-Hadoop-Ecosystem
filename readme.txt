Java Version 11.0.7

Apps used in this project:
1) Apache Hadoop v3.2.1
2) Apache Hive v3.1.2
3) Apache Flume v1.6.0

Before running the app make sure that:
1) Apache Hive is configured.
2) Apache Flume is configured.
2.1) Apache Flume configuration file "twitter.conf" containing the keyword search parameters as well the data sink parameters are clearly defined and stored in "/path/to/flume/conf" folder (using the Apache Flume configuration present in the Project Files folder in this project is recoomended).
3) Apache Hive server 2 configured and running.
4) Hadoop services are configured and running.
5) A table named "dictionary" is created in hive with AFINN-111.txt loaded into the "dictionary" table.
6) Apache Flume is storing twitter data into hdfs in the "/user/Hadoop/twitter-data" folder.
7) Apache Flume "twitter.conf" path in the Java application may need to be modified.
8) Store the latest version of twitter4j.jar in the Apache Flume directory.
9) JavaFX library is installed(it is recommended to use a Java version that already has the JavaFX module).

Troubleshooting:
1) Check the Hive and Hadoop configuration xml files with the ones including in the Project Files folder in this project.
2) Check if the correct Apache flume "twitter.conf" is set in the Java application.
3) Check to see if the run time configuration contains the following if JavaFX is installed as standalone:
  --module-path /path/to/javafx-sdk-x.x.x/lib --add-modules javafx.controls,javafx.fxml //needed for standalone JavaFX library
  -Xms1024M
  -Xmx4096M
  
Note:
There are some functionalities present in the project that is intended for developmental purposes only and may not work as intended or may not work at all.
You may have to modify the java code to properly run this application. 
External libraries required in the project classpath that is not present in your system may need to be downloadeded from the internet 
