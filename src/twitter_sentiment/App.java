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
    final int WINDOW_SIZE = 10;
    private ScheduledExecutorService scheduledExecutorService;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {
      //  primaryStage.setTitle("JavaFX Realtime Chart Demo");

        //defining the axes
     //   final CategoryAxis xAxis = new CategoryAxis(); // we are gonna plot against time
     //   final NumberAxis yAxis = new NumberAxis();
     //   xAxis.setLabel("Time/s");
     //   xAxis.setAnimated(false); // axis animations are removed
      //  yAxis.setLabel("Value");
      //  yAxis.setAnimated(false); // axis animations are removed
         CategoryAxis xAxis = new CategoryAxis();
         NumberAxis yAxis = new NumberAxis();
         yAxis.setAnimated(false);
         final BarChart<String,Number> bc = new BarChart<String,Number>(xAxis,yAxis);
         XYChart.Series<String, Number> series1 = new XYChart.Series<>();
         XYChart.Series<String, Number> series2 = new XYChart.Series<>();
         XYChart.Series<String, Number> series3 = new XYChart.Series<>();
        //creating the line chart with two axis created above
     //   final LineChart<String, Number> lineChart = new LineChart<>(xAxis, yAxis);
         stage.setTitle("Bar Chart Sample");
         
         bc.setTitle("Sentiment Analysis");
        
         xAxis.setLabel("Company");       
         yAxis.setLabel("Sentiment Count");
         
         series1.setName("Postive");       
        
         series2.setName("Nuetral");
         
         series3.setName("Negative"); 
      //  lineChart.setTitle("Realtime JavaFX Charts");
      //  lineChart.setAnimated(false); // disable animations

        //defining a series to display data
       // XYChart.Series<String, Number> series = new XYChart.Series<>();
       // series.setName("Data Series");

        // add series to chart
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

        // setup scene
        

        // this is used to display time in HH:mm:ss format
     //   final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

        // setup a scheduled executor to periodically put data into the chart
      //  scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        // put dummy data onto graph per second
     //  scheduledExecutorService.scheduleAtFixedRate(() -> {
            // get a random integer between 0-10
          //  Integer random = ThreadLocalRandom.current().nextInt(10);
        Task task = new Task<Void>() {
        	
			@Override
			protected Void call() throws Exception {
				// TODO Auto-generated method stub
				while (true) {
	              
	                try {
	                	Thread.sleep(1000);
            			 Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
            			 Statement stmt = con.createStatement();
            			 
             			 stmt.execute("create external table if not exists load_tweets(id BIGINT, text STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION '/user/Hadoop/twitter_data'");
            	          stmt.execute("LOAD DATA INPATH '/user/Hadoop/twitter_data' INTO TABLE load_tweets");
            	          stmt.execute("drop table if exists split_words");
            	          stmt.execute("drop table if exists split ");
            	          stmt.execute("drop table if exists tweet_word");
            	          stmt.execute("drop table if exists key_word");
            	          stmt.execute("drop table if exists tweets_join");
            	        //  stmt.execute("drop table if exists dictionary");
            	          stmt.execute("drop table if exists word_join ");
            	          stmt.execute("drop table if exists rating_table ");
            	          stmt.execute("create table if not exists split as select id as id,REGEXP_REPLACE(text,'[^0-9A-Za-z]+',' ') as text from load_tweets");
            	          stmt.execute("create table if not exists split_words as select id as id,split(text,' ') as words from split");
            	          stmt.execute("create table if not exists tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word");
            	          stmt.execute("create table if not exists key_word stored as orc TBLPROPERTIES('transactional'='true') as select id as id, search from split_words LATERAL VIEW explode(words) w as search where search like \"%mazon%\" OR search like \"%bay%\" OR search like \"%Bay%\" OR search like \"%liexpress%\" ");
            	        //  stmt.execute("UPDATE key_word SET search = (case when search like '%mazon%'  then 'Amazon' when search like '%bay%' then 'eBay' when search like '%liepxress%' then 'Aliexpress' end) WHERE search in ('%mazon%', '%bay%', '%liepxress%')");
            	          stmt.execute("update key_word SET search = 'amazon' where search like \"%mazon%\" ");
            	          stmt.execute("update key_word SET search = 'eBay' where search like \"%bay%\" ");
            	          stmt.execute("update key_word SET search = 'aliexpress' where search like \"%liepxress%\" ");
            	          stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
            	          stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
            	          //stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
            	          stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
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
            	              if(search != null) {
            	            	  search.strip();
            	              
            	              // int rating = rs.getInt("rating");
            	            // System.out.println("dawdwa"+search+"wdad");
            	               
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
            	           // bc.getData().add(series1);
            	              
            	          //  System.out.println("\n\nCount0     Search");
            	            ResultSet rs2 = stmt.executeQuery("select COUNT(id), search from rating_table where rating = 0 GROUP BY search");
            	            while (rs2.next()) {
            	                int count = rs2.getInt("_c0");
            	                    //String word = rs.getString("word");
            	                //String search = rs.getString("search");
            	                //String rating = rs.getString("rating");
            	                String search = rs2.getString("search");
            	               if(search != null) {
             	            	  search = search.strip();
             	              
            	               // int rating = rs.getInt("rating");
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
            	          //  bc.getData().add(series2);
            	            //System.out.println("\n\nCount-     Search");
            	            
            	        
           				
            	           ResultSet rs3 = stmt.executeQuery("select COUNT(id), search from rating_table where rating < 0 GROUP BY search");
           			
           					// TODO Auto-generated catch block
           			
           					while (rs3.next()) {
           					    int count = rs3.getInt("_c0");
           					        //String word = rs.getString("word");
           					    //String search = rs.getString("search");
           					    //String rating = rs.getString("rating");
           					    String search = rs3.getString("search");
           					    if(search != null) {
           		 	            	  search = search.trim();
           		 	              
           					   // int rating = rs.getInt("rating");
           					    if(search.contains("amazon")) {
           					    	System.out.println(search);
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
           					    System.out.println(count+"    "+search);
           					    
           					 }
           					 System.out.println("Complete");
           					// bc.getData().setAll(series1,series2,series3);
           					con.close();
           				} catch (SQLException e) {
           					// TODO Auto-generated catch block
           					e.printStackTrace();
           				}
	                // UI update is run on the Application thread
	               // Platform.runLater(updater);
	            }
				//return null;
			}
			
        	
        };
        Thread tread = new Thread(task);
        tread.setDaemon(true);
        tread.start();
        
    	   Thread thread = new Thread(new Runnable() {

               @Override
               public void run() {
            	   try {
             			 Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
             			 Statement stmt = con.createStatement();
             			 
                         // Add a new number to the linechart
                       //  this.series.getData().add(new Data<>("",0));

                         // Remove first number of linechart
                        // this.series.getData().remove(0);
                         
                        //	 i = i+1;
                        	//  Data<String, Number> data = new XYChart.Data(Amazon, 0);
             			
                              
             			
            	          /*  	 series1.getData().set(0,new XYChart.Data(Amazon, 7000));
            	  	     		series1.getData().set(1,new XYChart.Data(eBay, -800));
            	  	     		 series1.getData().set(2,new XYChart.Data(Aliexpress, +98));
            	  	    		 series2.getData().set(0,new XYChart.Data(Amazon, -98));
            	  	     		series2.getData().set(1,new XYChart.Data(eBay, +1));
            	  	     		 series2.getData().set(2,new XYChart.Data(Aliexpress, 66));
            	  	    		series3.getData().set(0,new XYChart.Data(Amazon, -77));
            	  	    		series3.getData().set(1,new XYChart.Data(eBay, +10));
            	  	    		 series3.getData().set(2,new XYChart.Data(Aliexpress, +2)); */
            	  	    	//	bc.getData().add(series1);
            	  	  		// bc.getData().add(series2);
            	  	  		// bc.getData().add(series3);
             			 
             			 stmt.execute("create external table if not exists load_tweets(id BIGINT, text STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION '/user/Hadoop/twitter_data'");
             	          stmt.execute("LOAD DATA INPATH '/user/Hadoop/twitter_data' INTO TABLE load_tweets");
             	          stmt.execute("drop table if exists split_words");
             	          stmt.execute("drop table if exists split ");
             	          stmt.execute("drop table if exists tweet_word");
             	          stmt.execute("drop table if exists key_word");
             	          stmt.execute("drop table if exists tweets_join");
             	        //  stmt.execute("drop table if exists dictionary");
             	          stmt.execute("drop table if exists word_join ");
             	          stmt.execute("drop table if exists rating_table ");
             	          stmt.execute("create table if not exists split as select id as id,REGEXP_REPLACE(text,'[^0-9A-Za-z]+',' ') as text from load_tweets");
             	          stmt.execute("create table if not exists split_words as select id as id,split(text,' ') as words from split");
             	          stmt.execute("create table if not exists tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word");
             	          stmt.execute("create table if not exists key_word stored as orc TBLPROPERTIES('transactional'='true') as select id as id, search from split_words LATERAL VIEW explode(words) w as search where search like \"%mazon%\" OR search like \"%bay%\" OR search like \"%Bay%\" OR search like \"%liexpress%\" ");
             	          stmt.execute("UPDATE key_word SET search = (case when search like '%mazon%'  then 'Amazon' when search like '%bay%' then 'eBay' when search like '%liepxress%' then 'Aliexpress' end) WHERE search in ('%mazon%', '%bay%', '%liepxress%')");

             	          stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
             	          stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
             	          //stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
             	          stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
             	          stmt.execute("select id,AVG(rating) as rating from word_join GROUP BY word_join.id order by rating DESC");
             	          //while(output.next()) {
             	              //System.out.println(output.getString(1));
             	          //}
             	        //  stmt.execute("create table if not exists rating_table as select id as id,search as search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating");
             	           ResultSet rs = stmt.executeQuery("select COUNT(id), search from rating_table where rating > 0 GROUP BY search");
             	           // ResultSet rs = stmt.executeQuery("select id,search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating DESC");
             	            System.out.println("Count+     Search");
             	            
             	            while (rs.next()) {
             	               int count = rs.getInt("_c0");
             	                   //String word = rs.getString("word");
             	               //String search = rs.getString("search");
             	               //String rating = rs.getString("rating");
             	               String search = rs.getString("search");
             	              if(search != null) {
             	            	  search.strip();
             	              
             	              // int rating = rs.getInt("rating");
             	            // System.out.println("dawdwa"+search+"wdad");
             	               
             	               if(search.contains("Amazon")) {
             	            	  System.out.println("series1   "+search);
             	             	   series1.getData().set(0,new XYChart.Data(Amazon, count));
             	                }
             	                else if(search.contains("eBay")  || search.contains("ebay")) {
             	                	System.out.println("series1   "+search);
             	             	   series1.getData().set(1,new XYChart.Data(eBay, count));
             	                }
             	                else if(search.contains("Aliexpress")){
             	                	System.out.println("series1   "+search);
             	             	   series1.getData().set(2,new XYChart.Data(Aliexpress, count));
             	                }
             	              }
             	               System.out.println(count+"    "+search);
             	            }
             	           // bc.getData().add(series1);
             	              
             	          //  System.out.println("\n\nCount0     Search");
             	            ResultSet rs2 = stmt.executeQuery("select COUNT(id), search from rating_table where rating = 0 GROUP BY search");
             	            while (rs2.next()) {
             	                int count = rs2.getInt("_c0");
             	                    //String word = rs.getString("word");
             	                //String search = rs.getString("search");
             	                //String rating = rs.getString("rating");
             	                String search = rs2.getString("search");
             	               if(search != null) {
              	            	  search = search.strip();
              	              
             	               // int rating = rs.getInt("rating");
             	                if(search.contains("Amazon")) {
             	                	System.out.println("series2   "+search);
             	             	   series2.getData().set(0,new XYChart.Data(Amazon, count));
             	                }
             	                else if(search.contains("eBay") || search.contains("ebay")) {
             	                	System.out.println("series2   "+search);
             	             	   series2.getData().set(1,new XYChart.Data(eBay, count));
             	                }
             	                else if(search.contains("Aliexpress")){
             	                	System.out.println("series2   "+search);
             	             	   series2.getData().set(2,new XYChart.Data(Aliexpress, count));
             	                }
             	               }
             	                System.out.println(count+"    "+search);
             	             }
             	          //  bc.getData().add(series2);
             	            //System.out.println("\n\nCount-     Search");
             	            
             	        
            				
             	           ResultSet rs3 = stmt.executeQuery("select COUNT(id), search from rating_table where rating < 0 GROUP BY search");
            			
            					// TODO Auto-generated catch block
            			
            					while (rs3.next()) {
            					    int count = rs3.getInt("_c0");
            					        //String word = rs.getString("word");
            					    //String search = rs.getString("search");
            					    //String rating = rs.getString("rating");
            					    String search = rs3.getString("search");
            					    if(search != null) {
            		 	            	  search = search.trim();
            		 	              
            					   // int rating = rs.getInt("rating");
            					    if(search.contains("Amazon")) {
            					    	System.out.println(search);
            					  	   series3.getData().set(0,new XYChart.Data(Amazon, count));
            					     }
            					     else if(search.contains("eBay")|| search.contains("ebay")) {
            					    	 System.out.println("series3   "+search);
            					  	   series3.getData().set(1,new XYChart.Data(eBay, count));
            					     }
            					     else if(search.contains("Aliexpress")){
            					    	 System.out.println("series3   "+search);
            					  	   series3.getData().set(2,new XYChart.Data(Aliexpress, count));
            					     }
            					    }
            					    System.out.println(count+"    "+search);
            					    
            					 }
            					 System.out.println("Complete");
            					// bc.getData().setAll(series1,series2,series3);
            					con.close();
            				} catch (SQLException e) {
            					// TODO Auto-generated catch block
            					e.printStackTrace();
            				}
                   Runnable updater = new Runnable() {

                       @Override
                       public void run() {
                    	   bc.getData().setAll(series1,series2,series3);
                   			
                       }
                   };

                   while (true) {
                       try {
                           Thread.sleep(1000);
                       } catch (InterruptedException ex) {
                       }

                       // UI update is run on the Application thread
                       Platform.runLater(updater);
                   }
               }

           });
           // don't let thread prevent JVM shutdown
           //thread.setDaemon(true);
           //thread.start();
            // Update the chart
          //  Platform.runLater(() -> {
                // get current time
           Scene scene = new Scene(bc, 800, 600);
           stage.setScene(scene);

           // show the stage
           stage.show();
          //  });
       // }, 6, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        scheduledExecutorService.shutdownNow();
    }
}