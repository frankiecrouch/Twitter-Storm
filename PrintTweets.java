package udacity.storm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class TweetPrinterBolt extends BaseBasicBolt {
	
     @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	 
    	 //if the tweet is SPAM ==> put in a file called SPAM
    	 if(tuple.getStringByField("category").equals("SPAM")){  
			  try {
			    BufferedWriter output;
			   output = new BufferedWriter(new
			   FileWriter("/home/ubuntu/workspace/Stage6/SPAM.txt", true));
			
			   output.newLine();
			   output.append("NEW_TWEET*****************************************");
			   output.newLine();
			   output.append(tuple.getString(1));
			   output.close();
			
			      } catch (IOException e) { e.printStackTrace();}			  
		  }//end of if statement
    	 
    	 //else put the tweet in a file called tweets
    	 else{
    		 try {
 			    BufferedWriter output;
 			   output = new BufferedWriter(new
 			   FileWriter("/home/ubuntu/workspace/Stage6/tweets.txt", true));
 			  
			   output.newLine();
			   output.append("NEW_TWEET*****************************************");
			   output.newLine();
 			   output.append(tuple.getString(1));
 			   output.close();
 			   
 			      } catch (IOException e) { e.printStackTrace();}			  
 		  }//end of else statement		 	 
     }
  
     @Override
     public void declareOutputFields(OutputFieldsDeclarer ofd) {
     }
  
}