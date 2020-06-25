package twitter_sentiment;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.concurrent.Task;
import javafx.scene.Scene;
import javafx.scene.chart.PieChart;
import javafx.scene.control.Label;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledExecutorService;

public class pie_graph extends Application {
	final static String Amazon = "Amazon";
    final static String eBay = "eBay";
    final static String Aliexpress = "Aliexpress";
    private ScheduledExecutorService scheduledExecutorService;
    Connection con;
    Statement stmt;
        
    public static void main(String[] args) {
        launch(args);
    }

    public void start(Stage stage) throws Exception {
      
        //defining the axes
    	final PieChart chart = new PieChart();
        chart.setTitle("Tweets Count per Company");
    
        chart.setLabelsVisible(true);
       // chart.getData().add(new PieChart.Data("Iphone 5S", 13));
        final Label caption = new Label("");     
        
         Task<Void> task = new Task<Void>() {
        	
			@Override
			protected Void call() throws Exception {
				Thread.sleep(8000);
				//while (true) {
	              
	                try {
	                	  //create connection and connect to hiveserver2
	                	con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
	            		stmt = con.createStatement();
            			 /*
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
            	          stmt.execute("update key_word SET search = 'eBay' where search like \"%ebay%\" OR search like \"%eBay%\" OR search like \"%EBay%\" OR search like \"%Ebay%\"");
            	          stmt.execute("update key_word SET search = 'aliexpress' where search like \"%liexpress%\" OR search like \"%liExpress%\"");
            	          
            	          //create table tweets_join by joining tweet_word and key_word, this is done to include the search word used with each tweet
            	          stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
            	          //stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
            	          //stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
            	          
            	          //create table word_join by joining dictionary and tweets_join, this is done so that almost each tweet words would now have a sentiment value
            	          stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
            	          
            	          //create table rating_table to average out the sentiment value of the tweet words
            	          stmt.execute("create table if not exists rating_table as select id as id,search as search,AVG(rating) as rating from word_join GROUP BY word_join.id, search order by rating");      
            	            */      	         
            	          ResultSet rs = stmt.executeQuery("select COUNT(id), search from rating_table WHERE search IS NOT NULL GROUP BY search");
            	           
            	       
            	            while (rs.next()) {
            	              int count = rs.getInt("_c0");
            	  
            	              String search = rs.getString("search");
            	              
            	              if(search != null) {
            	            	  search.strip();
            	               //Update the chart
            	               if(search.contains("amazon")) {
            	            	   	System.out.println("series1   "+search);
            	            	  	Platform.runLater(() -> {
            	            	  		chart.getData().add(new PieChart.Data(search, count));
            	            	  	});
            	                }
            	                else if(search.contains("eBay")  || search.contains("ebay")) {
            	                	System.out.println("series1   "+search);
            	                  	Platform.runLater(() -> {
            	                  		chart.getData().add(new PieChart.Data(search, count));
            	                  	});
            	                }
            	                else if(search.contains("aliexpress")){
            	                	System.out.println("series1   "+search);
            	                	Platform.runLater(() -> {
            	                		chart.getData().add(new PieChart.Data(search, count));
            	                	});
            	                }
            	              }
            	               System.out.println(count+"    "+search);
            	            }
            	           /// String text = "";
            	            
            	            Platform.runLater(() -> { 	
            	            	
            	            	
            	            	for (final PieChart.Data data : chart.getData()) {
            	            		double total = 0;
            	            		for (PieChart.Data d : chart.getData()) {
        	                            total += d.getPieValue();
        	                        }
            	            		String text = String.format("%.1f%%", 100*data.getPieValue()/total) ;
	            	                data.nameProperty().bind(
	            	                        Bindings.concat(
	            	                                data.getName(), " ", text , ""
	            	                        )
	            	                );
            	            	}
            	            });
            	            
           					System.out.println("Complete");
           					
           				} catch (SQLException e) {
           					// TODO Auto-generated catch block
           					e.printStackTrace();
           				}
	                // UI update is run on the Application thread
	               // Platform.runLater(updater);
	        //    }
					return null;
			}
			
        	
        };
        
        Thread tread = new Thread(task);
        // don't let thread prevent JVM shutdown
        tread.setDaemon(true);
        //thread start
        tread.start();
        
       // FlowPane root = new FlowPane();
       // root.getChildren().addAll(bc,bc2);
        
        //create new thread
        
        
       /// root.getChildren().addAll(chart);
        // setup scene
        Scene scene = new Scene(chart, 900, 900);
        stage.setScene(scene);
        // show the stage
        stage.show();
    }

    @Override
    public void stop() throws Exception {
    	con.close();
        super.stop();
        scheduledExecutorService.shutdownNow();
    }

}