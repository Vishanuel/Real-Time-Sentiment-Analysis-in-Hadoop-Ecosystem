/**
* Twitter Sentiment Analysis Program For Conducting Tweet Analysis that can:
* Start Hadoop & Yarn services
* Retrieve tweet data into HDFS
* @author  Naval Kumar, Vishanuel Prasad, Shimneet Chand
* @version 1.0
* @since   2020-15-06
*/
package twitter_sentiment;
import org.apache.flume.node.Application;
import org.apache.log4j.BasicConfigurator;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 *
 * @author s11146921
 */
public class Main {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
        
    public printOutput getStreamWrapper(InputStream is, String type)
    {
        return new printOutput(is, type);
    }
    
    public static void FormatNameNode() 
    {
        Runtime rt = Runtime.getRuntime();
        Main rte = new Main();
        printOutput errorReported, outputMessage;

        try 
        {
            Process proc = rt.exec("/usr/local/hadoop-2.7.3/bin/hdfs namenode -format");
            errorReported = rte.getStreamWrapper(proc.getErrorStream(), "ERROR");
            outputMessage = rte.getStreamWrapper(proc.getInputStream(), "OUTPUT");
            errorReported.start();
            outputMessage.start();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
        
    public static void StartHadoopServices() 
    {
        Runtime rt = Runtime.getRuntime();
        Main rte = new Main();
        printOutput errorReported, outputMessage;

        try 
        {
            Process proc = rt.exec("/usr/local/hadoop-3.2.1/sbin/start-all.sh");
            errorReported = rte.getStreamWrapper(proc.getErrorStream(), "ERROR");
            outputMessage = rte.getStreamWrapper(proc.getInputStream(), "OUTPUT");
            errorReported.start();
            outputMessage.start();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
    
    public static void StartTweetFetching()
    {
        String[] args1 = new String[] { "agent","-nTwitterAgent","-f/home/s11148140/apache-flume-1.6.0-bin/conf/twitter.conf" };
        BasicConfigurator.configure();
        Application.main(args1);
    }
    
        public static void StartHiveServer() 
    {
        Runtime rt = Runtime.getRuntime();
        Main rte = new Main();
        printOutput errorReported, outputMessage;

        try 
        {
            Process proc = rt.exec("hiveserver2" );
            errorReported = rte.getStreamWrapper(proc.getErrorStream(), "ERROR");
            outputMessage = rte.getStreamWrapper(proc.getInputStream(), "OUTPUT");
            errorReported.start();
            outputMessage.start();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
    
    public static void DropTable() throws SQLException
    {
        // TODO Auto-generated method stub
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
        Statement stmt = con.createStatement();
       
        stmt.execute("drop table if exists split_words");
        stmt.execute("drop table if exists tweet_word");
        stmt.execute("drop table if exists key_word");
        stmt.execute("drop table if exists tweets_join");
        //stmt.execute("drop table if exists dictionary");
        stmt.execute("drop table if exists word_join ");
        stmt.execute("drop table if exists rating_table ");
        con.close();
    }
    
    public static void StartHiveProcess() throws SQLException
    {
        // TODO Auto-generated method stub
    	
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
        Statement stmt = con.createStatement();
        //stmt.executeQuery(arg0);
        /*
        stmt.execute("drop table if exists split_words");
        stmt.execute("drop table if exists tweet_word");
        stmt.execute("drop table if exists key_word");
        stmt.execute("drop table if exists tweets_join");
        stmt.execute("drop table if exists dictionary");
        stmt.execute("drop table if exists word_join ");
       */
        
        stmt.execute("create external table if not exists load_tweets(id BIGINT, text STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION '/user/Hadoop/twitter_data'");
        stmt.execute("LOAD DATA INPATH '/user/Hadoop/twitter_data' INTO TABLE load_tweets");
        stmt.execute("drop table if exists split_words");
        stmt.execute("drop table if exists split ");
        stmt.execute("drop table if exists tweet_word");
        stmt.execute("drop table if exists key_word");
        stmt.execute("drop table if exists tweets_join");
       // stmt.execute("drop table if exists dictionary");
        stmt.execute("drop table if exists word_join ");
        stmt.execute("drop table if exists rating_table ");
        stmt.execute("create table if not exists split as select id as id,REGEXP_REPLACE(text,'[^0-9A-Za-z]+',' ') as text from load_tweets");
        stmt.execute("create table if not exists split_words as select id as id,split(text,' ') as words from split");
        stmt.execute("create table if not exists tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word");
        stmt.execute("create table if not exists key_word stored as orc TBLPROPERTIES('transactional'='true') as select id as id, search from split_words LATERAL VIEW explode(words) w as search where search like \"%mazon%\" OR search like \"%bay%\" OR search like \"%Bay%\" OR search like \"%liexpress%\" ");
        stmt.execute("update key_word SET search = 'amazon' where search like \"%mazon%\" ");
        stmt.execute("update key_word SET search = 'eBay' where search like \"%bay%\" ");
        stmt.execute("update key_word SET search = 'aliexpress' where search like \"%liepxress%\" ");
        stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
        stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
      //  stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
        stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
        //stmt.execute("select id,AVG(rating) as rating from word_join GROUP BY word_join.id order by rating DESC");
        //while(output.next()) {
            //System.out.println(output.getString(1));
        //}
        stmt.execute("create table if not exists rating_table as select id as id,search as search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating");
         ResultSet rs = stmt.executeQuery("select COUNT(id), search from rating_table where rating > 0 GROUP BY search");
         // ResultSet rs = stmt.executeQuery("select id,search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating DESC");
          System.out.println("Count+     Search");

          while (rs.next()) {
             int count = rs.getInt("_c0");
                 //String word = rs.getString("word");
             //String search = rs.getString("search");
             //String rating = rs.getString("rating");
             String search = rs.getString("search");
            // int rating = rs.getInt("rating");
             System.out.println(count+"    "+search);
          }
          System.out.println("\n\nCount0     Search");
          ResultSet rs2 = stmt.executeQuery("select COUNT(id), search from rating_table where rating = 0 GROUP BY search");
          while (rs2.next()) {
              int count = rs2.getInt("_c0");
                  //String word = rs.getString("word");
              //String search = rs.getString("search");
              //String rating = rs.getString("rating");
              String search = rs2.getString("search");
             // int rating = rs.getInt("rating");
              System.out.println(count+"    "+search);
           }
           
          System.out.println("\n\nCount-     Search");
          ResultSet rs3 = stmt.executeQuery("select COUNT(id), search from rating_table where rating < 0 GROUP BY search");
          while (rs3.next()) {
              int count = rs3.getInt("_c0");
                  //String word = rs.getString("word");
              //String search = rs.getString("search");
              //String rating = rs.getString("rating");
              String search = rs3.getString("search");
             // int rating = rs.getInt("rating");
              System.out.println(count+"    "+search);
           }
        con.close();
    }
    
    /**
     * @param args the command line arguments
     */       
    public static void main(String[] args) throws Exception 
    {
        int input;               
        do
        {
            System.out.println("--------------Twitter Sentiment Analysis------------");
            System.out.println("Enter '1' to format namenode(if required)");  
            System.out.println("Enter '2' to start HADOOP & YARN");
            System.out.println("Enter '3' to begin fetching tweets into HDFS");
            System.out.println("Enter '4' to start hiveserver2");
            System.out.println("Enter '5' to drop tables");
            System.out.println("Enter '6' to process data");
            System.out.println("Enter '10' to exit");
            //System.out.print(">>");
            
            // Get user's input and perform the operations
            Scanner u_input = new Scanner(System.in);
            while(!u_input.hasNextInt() || (!u_input.hasNext("[123456]")))
            {
                System.out.println("Invalid entry");
                System.out.println("Please enter a valid number from the choices listed above");
                u_input.next();
                //System.out.print(">>"); 
            }
            input = u_input.nextInt();
            
            switch (input) 
            {
                case 1://Necessary if booting for 1st time
                     FormatNameNode();
                break;
                
                case 2://Start Hadoop & Yarn Services
                     StartHadoopServices();
                break;
                  
                case 3://Fetching tweets using Flume agent
                    StartTweetFetching();
                break;
                case 4://Start Hive server
                    StartHiveServer();
                break;
                case 5://Drop tables
                    DropTable();
                break;
                case 6://Process Data using Hive
                    StartHiveProcess();
                break;
            }
        }while(input != 10);
    }
    
    private class printOutput extends Thread
    {
        InputStream is = null;

        printOutput(InputStream is, String type) 
        {
            this.is = is;
        }

        public void run() 
        {
            String s = null;
            try 
            {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                while ((s = br.readLine()) != null) 
                {
                    System.out.println(s);
                }
            } 
            catch (IOException ioe) 
            {
                ioe.printStackTrace();
            }
        }
    }
    
}
