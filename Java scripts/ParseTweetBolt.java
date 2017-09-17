package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * A bolt that categorises the tweets
 */
public class CategoryBolt extends BaseRichBolt 
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector) 
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) 
  {
    // get the 'tweet_with_category' and split it by the word DELIMITER and get the tweet    
	 String tweet = tuple.getStringByField("tweet_with_category").split("DELIMITER")[0];
	 
    // provide the delimiters for splitting the tweet
    String delims = "[ .,?!]+";

    // now split the tweet into words 
    //to be able to iterate through each word of the tweet
    String[] words = tweet.split(delims);
    
    //create a list of SPAM words
    String spam_words = "vaccine,shot,seasonal,outbreak,jab,influenza," +
    					"effect,free,healthy,soothe,remedy,effective," +
    					"illness,information,dog,symptom,system,protect" +
    					"you,learn,treat,immune";
    
    //split into tokens
    String[] spam_check = spam_words.split(",");
    
    //initiate the category to null
    String category = "null";
    
    // for each word in the tweet
    mainloop:
    for (String word: words) {  //iterate through each tweet word    	
    	for(String spam: spam_check ){ //iterate through each SPAM word
    		if(word.toLowerCase().contains(spam)){
    			category = "SPAM"; //if there is a match ==> tweet is considered to be SPAM
    			collector.emit(new Values(category, tweet)); //emit the output    			
    			break mainloop;//stop the for loop    			
    		}//end of if statement
    	}//end of second for statement
    }//end of first for statement
    
    //if the tweet is not considered to be SPAM
    if(category.equals("null")){
    	
    	//find what data is available and categorize as:
    	//NO_INFO, OUTSIDE_UK, LONDON, REST_OF_UK
    	String dataAvailable = tuple.getStringByField("tweet_with_category").split("DELIMITER")[1];
    		
    		if(dataAvailable.equals("noGeoInfo")){
    			category = "NO_INFO";
        		collector.emit(new Values(category, tweet));    			
    		}

    		else if(dataAvailable.equals("place")){
    			String country = tuple.getStringByField("tweet_with_category").split("DELIMITER")[2].split(",")[0];
    			
                //if not in the UK set as outside_uk
    			if(!country.equals("GB")){
    				category = "OUTSIDE_UK";
            		collector.emit(new Values(category, tweet));    				
    			}
    			
                //else check if in London or not
    			else{
    				String london_check = tuple.getStringByField("tweet_with_category").split("DELIMITER")[2].split(",")[2];
    				if(london_check.equals(" London")){
    					category = "LONDON";
                		collector.emit(new Values(category, tweet));    					
    				}//end of if in london
    				
    				else{
    					category = "REST_OF_UK";
                		collector.emit(new Values(category, tweet));
    				}
    			}
    		}//end of place section 
    		
    		if(dataAvailable.equals("coordinates")){
    			double latitude = 
                Double.parseDouble(tuple.getStringByField("tweet_with_category").split("DELIMITER")[2].split(",")[0]);

    			double longitude = 
                Double.parseDouble(tuple.getStringByField("tweet_with_category").split("DELIMITER")[2].split(",")[1]);
    			
                //circle around London
    			double center_london_lat = 51.5073509;
    			double center_london_long = -0.12775829999998223;
    			double r_london = 0.22458;
    			
                //circle around the UK
    			double center_uk_lat = 55.378051;
    			double center_uk_long = -3.43597299999999;
    			double r_UK = 5.38987;
    			
                //check if coordinates are in London
    			if ((longitude - center_london_long)*(longitude - center_london_long) + 
	      				(latitude - center_london_lat)*(latitude - center_london_lat) < r_london*r_london){
    				category = "LONDON";
            		collector.emit(new Values(category, tweet));  
    			}/
    			
                //else check if coordinates are in the UK
    			else if ((longitude - center_uk_long)*(longitude - center_uk_long) + 
	      				(latitude - center_uk_lat)*(latitude - center_uk_lat) < r_UK*r_UK){
    				category = "REST_OF_UK";
            		collector.emit(new Values(category, tweet));  
    			}
    			
    			else{
    				category = "OUTSIDE_UK";
            		collector.emit(new Values(category, tweet));     				
    			}   			
    		}//end of coordinates section
    }//end of category = null statement
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) 
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of two columns called 'category' and 'tweet'
    declarer.declare(new Fields("category", "tweet"));
  }
}
