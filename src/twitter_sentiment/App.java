package twitter_sentiment;


import javafx.application.Application;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.chart.XYChart.Series;
import javafx.stage.Stage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class App extends Application {
	final static String Amazon = "Amazon";
    final static String eBay = "eBay";
    final static String Aliexpress = "Aliexpress";
    private ScheduledExecutorService scheduledExecutorService;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {
      
        //defining the axes
         CategoryAxis xAxis = new CategoryAxis();
         NumberAxis yAxis = new NumberAxis();
         yAxis.setAnimated(false); // axis animations are removed
         xAxis.setAnimated(false); // axis animations are removed
         
         //create bar chart
         final BarChart<String,Number> bc = new BarChart<String,Number>(xAxis,yAxis);
         
         //defining a series to display data
         XYChart.Series<String, Number> series1 = new XYChart.Series<>();
         XYChart.Series<String, Number> series2 = new XYChart.Series<>();
         XYChart.Series<String, Number> series3 = new XYChart.Series<>();

         stage.setTitle("Bar Chart Sample");
         
         bc.setTitle("Sentiment Analysis");
         // disable animations
         bc.setAnimated(false);
         xAxis.setLabel("Company");       
         yAxis.setLabel("Sentiment Count");
         
         series1.setName("Postive");              
         series2.setName("Neutral");        
         series3.setName("Negative"); 

         // add series to chart
         // put dummy data onto graph 
         series1.getData().add(new XYChart.Data(Amazon, 0));
   		series1.getData().add(new XYChart.Data(eBay, 0));
   		 series1.getData().add(new XYChart.Data(Aliexpress, 0));
  		 series2.getData().add(new XYChart.Data(Amazon, 0));
   		series2.getData().add(new XYChart.Data(eBay, 0));
   		 series2.getData().add(new XYChart.Data(Aliexpress, 0));
  		series3.getData().add(new XYChart.Data(Amazon, 0));
  		series3.getData().add(new XYChart.Data(eBay, 0));
  		 series3.getData().add(new XYChart.Data(Aliexpress, 0));
        bc.getData().addAll(series1,series2,series3);

        
        Task task = new Task<Void>() {
        	
			@Override
			protected Void call() throws Exception {
				Thread.sleep(8000);
				while (true) {
	              
	                try {
	                	  //create connection and connect to hiveserver2
            			  Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
            			  Statement stmt = con.createStatement();
            			 
            			  //create table load_tweets and load in twitter data
             			  stmt.execute("create external table if not exists load_tweets(id BIGINT, text STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION '/user/Hadoop/twitter_data'");
            	          
             			  //load twitter data in load_tweets table
             			  stmt.execute("LOAD DATA INPATH '/user/Hadoop/twitter_data' INTO TABLE load_tweets");
            	          
             			  //drop tables for iterative analysis
             			  stmt.execute("drop table if exists split_words");
            	          stmt.execute("drop table if exists split ");
            	          stmt.execute("drop table if exists tweet_word");
            	          stmt.execute("drop table if exists key_word");
            	          stmt.execute("drop table if exists tweets_join");
            	          stmt.execute("drop table if exists word_join ");
            	          stmt.execute("drop table if exists rating_table ");
            	          
            	          //create table split and store clean twitter text by removing symbols, emojis, etc.
            	          stmt.execute("create table if not exists split as select id as id,REGEXP_REPLACE(text,'[^0-9A-Za-z]+',' ') as text from load_tweets");
            	          
            	          //create table split_words and store tokenized tweets.
            	          stmt.execute("create table if not exists split_words as select id as id,split(text,' ') as words from split");
            	          
            	          //create table tweet_word with each word in the tweets stored in seperate rows.
            	          stmt.execute("create table if not exists tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word");
            	          
            	          stmt.execute("create table if not exists key_word stored as orc TBLPROPERTIES('transactional'='true') as select id as id, search from split_words LATERAL VIEW explode(words) w as search where search like \"%mazon%\" OR search like \"%bay%\" OR search like \"%Bay%\" OR search like \"%liexpress%\" ");
            	          //stmt.execute("UPDATE key_word SET search = (case when search like '%mazon%'  then 'amazon' when search like '%bay%' then 'eBay' when search like '%liepxress%' then 'aliexpress' end) WHERE search in ('%mazon%', '%bay%', '%liepxress%')");
            	          
            	          //update key_word table and set the search
            	          stmt.execute("update key_word SET search = 'amazon' where search like \"%mazon%\" ");
            	          stmt.execute("update key_word SET search = 'eBay' where search like \"%bay%\" ");
            	          stmt.execute("update key_word SET search = 'aliexpress' where search like \"%liepxress%\" ");
            	          
            	          //create table tweets_join by joining tweet_word and key_word, this is done to include the search word used with each tweet
            	          stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
            	          //stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
            	          //stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
            	          
            	          //create table word_join by joining dictionary and tweets_join, this is done so that almost each tweet words would now have a sentiment value
            	          stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
            	          
            	          //create table rating_table to average out the sentiment value of the tweet words
            	          stmt.execute("create table if not exists rating_table as select id as id,search as search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating");      
            	          ResultSet rs = stmt.executeQuery("select COUNT(id), search from rating_table where rating > 0 GROUP BY search");
            	           
            	       
            	            while (rs.next()) {
            	              int count = rs.getInt("_c0");
            	  
            	              String search = rs.getString("search");
            	              
            	              if(search != null) {
            	            	  search.strip();
            	               //Update the chart
            	               if(search.contains("amazon")) {
            	            	   	System.out.println("series1   "+search);
            	            	  	Platform.runLater(() -> {
            	            	  		series1.getData().add(new XYChart.Data(Amazon, count));
            	            	  	});
            	                }
            	                else if(search.contains("eBay")  || search.contains("ebay")) {
            	                	System.out.println("series1   "+search);
            	                  	Platform.runLater(() -> {
            	                  		series1.getData().add(new XYChart.Data(eBay, count));
            	                  	});
            	                }
            	                else if(search.contains("aliexpress")){
            	                	System.out.println("series1   "+search);
            	                	Platform.runLater(() -> {
            	                		series1.getData().add(new XYChart.Data(Aliexpress, count));
            	                	});
            	                }
            	              }
            	               System.out.println(count+"    "+search);
            	            }

            	            ResultSet rs2 = stmt.executeQuery("select COUNT(id), search from rating_table where rating = 0 GROUP BY search");
            	            while (rs2.next()) {
            	               int count = rs2.getInt("_c0");

            	               String search = rs2.getString("search");
            	               if(search != null) {
             	            	  search = search.strip();
             	              
             	            	//Update the chart
            	                if(search.contains("amazon")) {
            	                	System.out.println("series2   "+search);
            	                	Platform.runLater(() -> {
            	                		series2.getData().add(new XYChart.Data(Amazon, count));
            	                	});
            	                }
            	                else if(search.contains("eBay") || search.contains("ebay")) {
            	                	System.out.println("series2   "+search);
            	                	Platform.runLater(() -> {
            	                		series2.getData().add(new XYChart.Data(eBay, count));
            	                	});
            	                }
            	                else if(search.contains("aliexpress")){
            	                	System.out.println("series2   "+search);
            	                	Platform.runLater(() -> {
            	                		series2.getData().add(new XYChart.Data(Aliexpress, count));
            	                	 });
            	                }
            	               }
            	                System.out.println(count+"    "+search);
            	             }
           				
            	           ResultSet rs3 = stmt.executeQuery("select COUNT(id), search from rating_table where rating < 0 GROUP BY search");
	
           					while (rs3.next()) {
           					    int count = rs3.getInt("_c0");
     
           					    String search = rs3.getString("search");
           					    if(search != null) {
           		 	            	  search = search.trim();
           		 	            	  
           		 	            //Update the chart
           					     if(search.contains("amazon")) {
           					    	System.out.println("series3   "+search);
           					    	Platform.runLater(() -> {
           					    		series3.getData().add(new XYChart.Data(Amazon, count));
           					    	});
           					     }
           					     else if(search.contains("eBay")|| search.contains("ebay")) {
           					    	System.out.println("series3   "+search);
           					    	Platform.runLater(() -> {
           					    		series3.getData().add(new XYChart.Data(eBay, count));
           					    	});
           					     }
           					     else if(search.contains("aliexpress")){
           					    	System.out.println("series3   "+search);
           					    	Platform.runLater(() -> {
           					    		series3.getData().add(new XYChart.Data(Aliexpress, count));
           					    	});
           					     }
           					    }
           					   
           					    
           					 }
           					System.out.println("Complete");
           			
           					con.close();
           				} catch (SQLException e) {
           					// TODO Auto-generated catch block
           					e.printStackTrace();
           				}
	                // UI update is run on the Application thread
	               // Platform.runLater(updater);
	            }
			}
			
        	
        };
        //create new thread
        Thread tread = new Thread(task);
        // don't let thread prevent JVM shutdown
        tread.setDaemon(true);
        //thread start
        tread.start();
        
        // setup scene
        Scene scene = new Scene(bc, 800, 600);
        stage.setScene(scene);
        // show the stage
        stage.show();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        scheduledExecutorService.shutdownNow();
    }
}