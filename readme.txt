Run time configuration if installing JavaFX(it is packaged in oracle Java 8 version) as standalone.

--module-path /home/s11148140/javafx-sdk-11.0.2/lib --add-modules javafx.controls,javafx.fxml
-Xms1024M
-Xmx4096M


JavaFX is needed to run this project(you can install and use oracle Java 8 which will include this library).

Before running the app make sure that:
Apache Hive is configured.
Apache Flume is configured.
Hive server 2 is installed and configured and running.
Hadoop services are configured and running.
There is a "twitter.conf" file in "/path/to/apacheflume/conf" folder.
A table named "dictionary" is created in hive with AFINN-111.txt loaded into the "dictionary" table.
Apache Flume is storing twitter data in hdfs in the "/user/Hadoop/twitter-data" folder.
You need to modify the Apache Flume "twitter.conf" path in the Java application.


Troubleshooting:
Check the Hive and Hadoop configuration xml files with the ones including in the Project Files folder in this project.
Check if the correct Apache flume "twitter.conf" is set in the Java application.
