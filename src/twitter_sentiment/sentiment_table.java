package twitter_sentiment;


import javafx.application.Application;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.scene.Scene;

import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledExecutorService;


public class sentiment_table extends Application {
	final static String Amazon = "Amazon";
    final static String eBay = "eBay";
    final static String Aliexpress = "Aliexpress";
    private ScheduledExecutorService scheduledExecutorService;
    Connection con;
    Statement stmt;
    
    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {
      
        
    	TableView<Sentiment> tableViewAmazon = new TableView<Sentiment>();
        TableView<Sentiment> tableVieweBay = new TableView<Sentiment>();
        
        TableView<Sentiment> tableViewAliexpress= new TableView<Sentiment>();
        
        TableColumn<Sentiment, String> column0 = new TableColumn<Sentiment,String>("Search");
        column0.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("search"));
        
        TableColumn<Sentiment, Integer> column1 = new TableColumn<Sentiment, Integer>("Rating");
        column1.setCellValueFactory(new PropertyValueFactory<Sentiment, Integer>("rating"));

        TableColumn<Sentiment, String> column2 = new TableColumn<Sentiment, String>("Text");
        column2.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("text"));
        
        TableColumn<Sentiment, String> column10 = new TableColumn<Sentiment, String>("Search");
        column10.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("search"));
        
        TableColumn<Sentiment, Integer> column11 = new TableColumn<Sentiment, Integer>("Rating");
        column11.setCellValueFactory(new PropertyValueFactory<Sentiment, Integer>("rating"));

        TableColumn<Sentiment, String> column12 = new TableColumn<Sentiment, String>("Text");
        column12.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("text"));
        
        TableColumn<Sentiment, String> column20 = new TableColumn<Sentiment, String>("Search");
        column20.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("search"));
        
        TableColumn<Sentiment, Integer> column21 = new TableColumn<Sentiment, Integer>("Rating");
        column21.setCellValueFactory(new PropertyValueFactory<Sentiment, Integer>("rating"));

        TableColumn<Sentiment, String> column22 = new TableColumn<Sentiment, String>("Text");
        column22.setCellValueFactory(new PropertyValueFactory<Sentiment,String>("text"));

        tableVieweBay.getColumns().add(column0);
        tableVieweBay.getColumns().add(column1);
        tableVieweBay.getColumns().add(column2);  
        tableViewAmazon.getColumns().add(column10);
        tableViewAmazon.getColumns().add(column11);
        tableViewAmazon.getColumns().add(column12); 
        tableViewAliexpress.getColumns().add(column20);
        tableViewAliexpress.getColumns().add(column21);
        tableViewAliexpress.getColumns().add(column22); 
        //tableVieweBay.autosize();
        //tableViewAmazon.autosize();
        tableVieweBay.setMinHeight(300);
        tableVieweBay.setMinWidth(550);
        tableViewAmazon.setMinHeight(300);
        tableViewAmazon.setMinWidth(550);
        tableViewAliexpress.setMinHeight(300);
        tableViewAliexpress.setMinWidth(550);
        
      //  tableVieweBay.autosize();
      //  tableViewAliexpress.autosize();
      //  tableViewAmazon.autosize();
        
        
        final Label eBaylabel = new Label("Ebay 3 best and 3 worst Sentiments");
        final Label Amazonlabel = new Label("Amazon 3 best and 3 worst Sentiments");
        final Label Aliexpresslabel = new Label("Aliexpress 3 best and 3 worst Sentiments");
       
        
        VBox vbox = new VBox(); 
        vbox.setSpacing(15);
        vbox.setMinWidth(800);
        vbox.setPadding(new Insets(20, 20, 20, 20));
        vbox.getChildren().addAll(eBaylabel, tableVieweBay);
       
        
        vbox.getChildren().addAll(Amazonlabel, tableViewAmazon, Aliexpresslabel ,tableViewAliexpress);
        
        
        Scene scene = new Scene(vbox);
       
        
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
            			  stmt.execute("drop table if exists tweet_rating_table ");
            	          stmt.execute("create table if not exists tweet_rating_table as select rating_table.id,rating_table.search,rating_table.rating, load_tweets.text from rating_table JOIN load_tweets ON (load_tweets.id = rating_table.id)");    
            	          
            			  
            			  ResultSet rs2 = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'amazon' AND rating IS NOT NULL AND rating > 0 ORDER  BY rating DESC limit 3");
          	            while (rs2.next()) {
            	              int rating = rs2.getInt("rating");
            	              String search = rs2.getString("search");
            	              String text = rs2.getString("text");
            	              
            	              if(search != null) {
            	            	  search.strip();
            	            	  text.strip();
            	               //Update the chart
            	            
            	            	  Platform.runLater(() -> {
            	            		tableViewAmazon.getItems().add(new Sentiment(search, rating, text));
            	            	  });
            	                }
            	                
            	               System.out.println(text+"    "+search);
            	            }
            			  
            	          ResultSet rs = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'eBay' AND rating IS NOT NULL AND rating > 0 ORDER BY rating DESC limit 3");
            	           
            	          while (rs.next()) {
            	              int rating = rs.getInt("rating");
            	              String search = rs.getString("search");
            	              String text = rs.getString("text");
            	              
            	              if(search != null) {
            	            	  search.strip();
            	            	  text.strip();
            	               //Update the chart
            	            
            	            	  Platform.runLater(() -> {
            	            		  tableVieweBay.getItems().add(new Sentiment(search, rating, text));
            	            	  });
            	                }
            	                
            	               System.out.println("    "+search);
            	            }

            	            
           				
            	           ResultSet rs3 = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'aliexpress' AND rating IS NOT NULL AND rating > 0 ORDER BY rating DESC limit 3");
	
            	           while (rs3.next()) {
             	              int rating = rs3.getInt("rating");
             	              String search = rs3.getString("search");
             	              String text = rs3.getString("text");
             	              
             	              if(search != null) {
             	            	  search.strip();
             	            	  text.strip();
             	               //Update the chart
             	            
             	            	  Platform.runLater(() -> {
             	            		 tableViewAliexpress.getItems().add(new Sentiment(search, rating, text));
             	            	  });
             	                }
             	                
             	               System.out.println(text+"    "+search);
             	            }
            	           
            	           ResultSet rs4 = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'aliexpress' AND rating IS NOT NULL AND rating < 0 ORDER BY rating limit 3");
            	       	
            	           while (rs4.next()) {
             	              int rating = rs4.getInt("rating");
             	              String search = rs4.getString("search");
             	              String text = rs4.getString("text");
             	              
             	              if(search != null) {
             	            	  search.strip();
             	            	  text.strip();
             	               //Update the chart
             	            
             	            	  Platform.runLater(() -> {
             	            		 tableViewAliexpress.getItems().add(new Sentiment(search, rating, text));
             	            	  });
             	                }
             	                
             	               System.out.println("    "+search);
             	            }
            	           
            	           ResultSet rs5 = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'amazon' AND rating IS NOT NULL AND rating < 0 ORDER BY rating limit 3");
               	       	
            	           while (rs5.next()) {
             	              int rating = rs5.getInt("rating");
             	              String search = rs5.getString("search");
             	              String text = rs5.getString("text");
             	              
             	              if(search != null) {
             	            	  search.strip();
             	            	  text.strip();
             	               //Update the chart
             	            
             	            	  Platform.runLater(() -> {
             	            		 tableViewAmazon.getItems().add(new Sentiment(search, rating, text));
             	            	  });
             	                }
             	                
             	               System.out.println("    "+search);
             	            }
            	           
            	           ResultSet rs6 = stmt.executeQuery("select rating,text,search from tweet_rating_table where search = 'eBay' AND rating IS NOT NULL AND rating < 0 ORDER BY rating limit 3");
                  	       	
            	           while (rs6.next()) {
             	              int rating = rs6.getInt("rating");
             	              String search = rs6.getString("search");
             	              String text = rs6.getString("text");
             	              
             	              if(search != null) {
             	            	  search.strip();
             	            	  text.strip();
             	               //Update the chart
             	            
             	            	  Platform.runLater(() -> {
             	            		 tableVieweBay.getItems().add(new Sentiment(search, rating, text));
             	            	  });
             	                }
             	                
             	               System.out.println("    "+search);
             	            }
            	          
           					System.out.println("Complete");
           			
           					con.close();
           				} catch (SQLException e) {
           					// TODO Auto-generated catch block
           					e.printStackTrace();
           				}
	                // UI update is run on the Application thread
	               // Platform.runLater(updater);
	           // }
					return null;
			}
			
        	
        };
        //create new thread
        Thread tread = new Thread(task);
        // don't let thread prevent JVM shutdown
        tread.setDaemon(true);
        //thread start
        tread.start();
        
        // setup scene
        //Scene scene = new Scene(bc, 800, 600);
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