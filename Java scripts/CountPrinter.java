package udacity.storm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CountPrinterBolt extends BaseBasicBolt {
	
	private final String filename;
	
	public CountPrinter(String filename){
		this.filename = filename;
	}	
	
	
     @Override
     public void execute(Tuple tuple, BasicOutputCollector collector) {
   
   try {
     BufferedWriter output;
    output = new BufferedWriter(new
    FileWriter("/home/ubuntu/workspace/Stage6/"+ filename +".txt", true));
    int windowLenght = tuple.getInteger(2);
    double mins = (double)(windowLenght+1)/60;
    DecimalFormat df = new DecimalFormat("0");
     output.newLine();
     output.append("Count for the past " + df.format(mins) + " mintues ");
    output.append(": " + tuple.getString(0));
    output.append(" = ");
    output.append(Integer.toString(tuple.getInteger(1)));
    output.close();
       } catch (IOException e) { e.printStackTrace();}
   
      }
   
      @Override
      public void declareOutputFields(OutputFieldsDeclarer ofd) {
      }  
  
  
}
