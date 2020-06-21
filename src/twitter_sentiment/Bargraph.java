/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitter_sentiment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


//import org.apache.flume.node.Application;
import org.apache.log4j.BasicConfigurator;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;

import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.chart.XYChart.Data;
import javafx.scene.chart.XYChart.Series;
import javafx.stage.Stage;
import javafx.util.Duration;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;


/**
 *
 * @author Shimneet
 */


public class Bargraph extends Application {
    final static String Amazon = "Amazon";
    final static String eBay = "eBay";
    final static String Aliexpress = "Aliexpress";
  //  int k;
    int i;
  
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    final CategoryAxis xAxis = new CategoryAxis();
    final NumberAxis yAxis = new NumberAxis();
    final BarChart<String,Number> bc = 
        new BarChart<String,Number>(xAxis,yAxis);
    static XYChart.Series<String, Number> series1 = new Series<String, Number>();
    static XYChart.Series<String, Number> series2 = new Series<String, Number>();
    static XYChart.Series<String, Number> series3 = new Series<String, Number>();
    
    public void start(Stage stage) throws Exception{
    	  try {
              Class.forName(driverName);
          } catch (ClassNotFoundException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
          }
         
         
          
          /*
          stmt.execute("drop table if exists split_words");
          stmt.execute("drop table if exists tweet_word");
          stmt.execute("drop table if exists key_word");
          stmt.execute("drop table if exists tweets_join");
          stmt.execute("drop table if exists dictionary");
          stmt.execute("drop table if exists word_join ");
         */
          stage.setTitle("Bar Chart Sample");
          
          bc.setTitle("Sentiment Analysis");
          xAxis.setLabel("Company");       
          yAxis.setLabel("Sentiment Count");
          
          series1.setName("Postive");       
         
          series2.setName("Nuetral");
          
          series3.setName("Negative"); 
          
          
         
          
          Data<String, Number> data = new XYChart.Data(Amazon, 9);
         //series1.getData().add((Data<?, ?>) data);
   		series1.getData().add(new XYChart.Data(eBay, 0));
   		 series1.getData().add(new XYChart.Data(Aliexpress, 0));
  		 series2.getData().add(new XYChart.Data(Amazon, 0));
   		series2.getData().add(new XYChart.Data(eBay, 0));
   		 series2.getData().add(new XYChart.Data(Aliexpress, 90));
  		series3.getData().add(new XYChart.Data(Amazon, 0));
  		series3.getData().add(new XYChart.Data(eBay, 0));
  		 series3.getData().add(new XYChart.Data(Aliexpress, 0));
          bc.getData().add(series1);
 		 bc.getData().add(series2);
 		 bc.getData().add(series3);
 		  
 		Scene scene  = new Scene(bc,800,600);
  		 stage.setScene(scene);
   	        stage.show();
   	    // for(int k =0  ; k < 1; k++) {
   	    	 
 		
 	      
   	    // }
 	      
  	     // wait(10000);
  	      
      /*  
        XYChart.Series series2 = new XYChart.Series();
        series2.setName("2004");
        series2.getData().add(new XYChart.Data(austria, 57401.85));
        series2.getData().add(new XYChart.Data(brazil, 41941.19));
        series2.getData().add(new XYChart.Data(france, 45263.37));
  
        
        XYChart.Series series3 = new XYChart.Series();
        series3.setName("2005");
        series3.getData().add(new XYChart.Data(austria, 45000.65));
        series3.getData().add(new XYChart.Data(brazil, 44835.76));
        series3.getData().add(new XYChart.Data(france, 18722.18));
 */
  	   /* Timeline timeline = new Timeline(new KeyFrame(Duration.seconds(2), (event) -> {
  	    	for(int i = 0; i < 98; i++) {
  	    		
  	         	 // series1.getData().clear();
  	         	//series2.getData().clear();
  	         	//series3.getData().clear();
  	    	//	bc.getData().clear();
  	    		//System.out.print(series1.getData().size());
  	         	series1.getData().set(0,new XYChart.Data(Amazon, i-1));
  	     		series1.getData().set(1,new XYChart.Data(eBay, i-1));
  	     		 series1.getData().set(2,new XYChart.Data(Aliexpress, i+98));
  	    		 series2.getData().set(0,new XYChart.Data(Amazon, i-98));
  	     		series2.getData().set(1,new XYChart.Data(eBay, i+1));
  	     		 series2.getData().set(2,new XYChart.Data(Aliexpress, 66));
  	    		series3.getData().set(0,new XYChart.Data(Amazon, i-77));
  	    		series3.getData().set(1,new XYChart.Data(eBay, i+10));
  	    		 series3.getData().set(2,new XYChart.Data(Aliexpress, i+2));  	    	//	 bc.getData().set(0,  series3);
  	    	//	bc.getData().set(0,  series2);
  	    	//	bc.getData().set(0,  series1);
  	    		// bc.getData().add(series1);
  	    		// bc.getData().add(series2);
  	    		// bc.getData().add(series3);
  	    		// wait(10000);
  	    	       
  	    		
  	    	}
        }));*/
  	      
 	    //   Timeline tl = new Timeline();
  	        /*
 	      Timeline tl = new Timeline(
 	      new KeyFrame(Duration.millis(1000), 
 	          new EventHandler<ActionEvent>() {
 	              @Override public void handle(ActionEvent actionEvent) {
 	            	 for(int i =0  ; i < 988; i++) {
 	            	 series1.getData().set(0,new XYChart.Data(Amazon, i-1));
 	  	     		series1.getData().set(1,new XYChart.Data(eBay, i-1));
 	  	     		 series1.getData().set(2,new XYChart.Data(Aliexpress, i+98));
 	  	    		 series2.getData().set(0,new XYChart.Data(Amazon, i-98));
 	  	     		series2.getData().set(1,new XYChart.Data(eBay, i+1));
 	  	     		 series2.getData().set(2,new XYChart.Data(Aliexpress, 66));
 	  	    		series3.getData().set(0,new XYChart.Data(Amazon, i-77));
 	  	    		series3.getData().set(1,new XYChart.Data(eBay, i+10));
 	  	    		 series3.getData().set(2,new XYChart.Data(Aliexpress, i+2));  
 	                  for (Series<String, Number> series : bc.getData()) {
 	                      for (Data<String, Number> data : series.getData()) {
 	                          data.setYValue(Math.random() * 1000);
 	                          
 	                      }
 	                  }
 	  	    		 k = i;
 	  	    		
 	            	 }
 	              }
 	           }
 	      ));
 	     System.out.print(series1.getData());
 	    tl.setCycleCount(988);
 	      tl.setAutoReverse(false);
 	      tl.play();
 	      
 	   */
  	//  timeline.setCycleCount(9);
  //	timeline.setAutoReverse(false);
  //	timeline.play();
       
  	        update();
  	     

    }
    
    public void update() {
    	 Platform.runLater(() -> {
    		 
  			try {
  			 Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
  			 Statement stmt = con.createStatement();
  			 
              // Add a new number to the linechart
            //  this.series.getData().add(new Data<>("",0));

              // Remove first number of linechart
             // this.series.getData().remove(0);
              
             //	 i = i+1;
             	//  Data<String, Number> data = new XYChart.Data(Amazon, 0);
  			
                //   series1.getData().add(new XYChart.Data(Amazon, -1));
             //		series1.getData().add(new XYChart.Data(eBay, 1));
             //		 series1.getData().add(new XYChart.Data(Aliexpress, +98));
            	//	 series2.getData().add(new XYChart.Data(Amazon, i-98));
             //		series2.getData().add(new XYChart.Data(eBay, +1));
             //		 series2.getData().add(new XYChart.Data(Aliexpress, 66));
            	//	series3.getData().add(new XYChart.Data(Amazon, -77));
            	//	series3.getData().add(new XYChart.Data(eBay, +10));
            	//	 series3.getData().add(new XYChart.Data(Aliexpress, +2));
  			
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
  			 
  		//	 stmt.execute("create external table if not exists load_tweets(id BIGINT, text STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION '/user/Hadoop/twitter_data'");
  	      //    stmt.execute("LOAD DATA INPATH '/user/Hadoop/twitter_data' INTO TABLE load_tweets");
  	      //    stmt.execute("drop table if exists split_words");
  	      //    stmt.execute("drop table if exists split ");
  	       //   stmt.execute("drop table if exists tweet_word");
  	     //     stmt.execute("drop table if exists key_word");
  	         // stmt.execute("drop table if exists tweets_join");
  	        //  stmt.execute("drop table if exists dictionary");
  	      //    stmt.execute("drop table if exists word_join ");
  	       ///   stmt.execute("drop table if exists rating_table ");
  	       //   stmt.execute("create table if not exists split as select id as id,REGEXP_REPLACE(text,'[^0-9A-Za-z]+',' ') as text from load_tweets");
  	       //   stmt.execute("create table if not exists split_words as select id as id,split(text,' ') as words from split");
  	        //  stmt.execute("create table if not exists tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word");
  	       ///   stmt.execute("create table if not exists key_word stored as orc TBLPROPERTIES('transactional'='true') as select id as id, search from split_words LATERAL VIEW explode(words) w as search where search like \"%mazon%\" OR search like \"%bay%\" OR search like \"%Bay%\" OR search like \"%liexpress%\" ");
  	      //    stmt.execute("update key_word SET search = 'Amazon' where search like \"%mazon%\" ");
  	       //   stmt.execute("update key_word SET search = 'eBay' where search like \"%bay%\" ");
  	       //   stmt.execute("update key_word SET search = 'Aliexpress' where search like \"%liepxress%\" ");
  	       //   stmt.execute("create table if not exists tweets_join as select tweet_word.id, tweet_word.word,key_word.search from tweet_word LEFT OUTER JOIN key_word ON(tweet_word.id = key_word.id)");
  	         // stmt.execute("create table if not exists dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'");
  	          //stmt.execute("LOAD DATA INPATH '/user/AFINN-111.txt' into TABLE dictionary");
  	        //  stmt.execute("create table if not exists word_join as select tweets_join.id, tweets_join.word, tweets_join.search, dictionary.rating from tweets_join LEFT OUTER JOIN dictionary ON(tweets_join.word = dictionary.word)");
  	          //stmt.execute("select id,AVG(rating) as rating from word_join GROUP BY word_join.id order by rating DESC");
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
  	             	   series1.getData().add(new XYChart.Data(Amazon, count));
  	                }
  	                else if(search.contains("eBay")  || search.contains("ebay")) {
  	                	System.out.println("series1   "+search);
  	             	   series1.getData().add(new XYChart.Data(eBay, count));
  	                }
  	                else if(search.contains("Aliexpress")){
  	                	System.out.println("series1   "+search);
  	             	   series1.getData().add(new XYChart.Data(Aliexpress, count));
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
  	             	   series2.getData().add(new XYChart.Data(Amazon, count));
  	                }
  	                else if(search.contains("eBay") || search.contains("ebay")) {
  	                	System.out.println("series2   "+search);
  	             	   series2.getData().add(new XYChart.Data(eBay, count));
  	                }
  	                else if(search.contains("Aliexpress")){
  	                	System.out.println("series2   "+search);
  	             	   series2.getData().add(new XYChart.Data(Aliexpress, count));
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
 					  	   series3.getData().add(new XYChart.Data(Amazon, count));
 					     }
 					     else if(search.contains("eBay")|| search.contains("ebay")) {
 					    	 System.out.println("series3   "+search);
 					  	   series3.getData().add(new XYChart.Data(eBay, count));
 					     }
 					     else if(search.contains("Aliexpress")){
 					    	 System.out.println("series3   "+search);
 					  	   series3.getData().add(new XYChart.Data(Aliexpress, count));
 					     }
 					    }
 					    System.out.println(count+"    "+search);
 					    
 					 }
 					 System.out.println("Complete");
 					 bc.getData().setAll(series1,series2,series3);
 					con.close();
 				} catch (SQLException e) {
 					// TODO Auto-generated catch block
 					e.printStackTrace();
 				}
  			
  			
          });
    	
    }
 
    public static void main(String[] args) {
        Application.launch(args);
        
       
    }
    
    public class Task extends Thread {

        private boolean isActive = true;


        @Override
        public void run() {

            while (isActive) {

                try {
                    // Simulate heavy processing stuff
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
                FutureTask<Object> task = new FutureTask<Object>(new Callable<Object>()     {
                    @Override
                    public Object call() throws Exception {
                           // Add a new number to the linechart
                          // series.getData().add(new Data<>("",0));

                           // Remove first number of linechart
                         //  series.getData().remove(0);
                        return true;
                    }
                });
                // Add a new number to the linechart

                // Remove first number of linechart
            }
        }

        public void  kill(){
            isActive = false;
        }

    }
    
}


