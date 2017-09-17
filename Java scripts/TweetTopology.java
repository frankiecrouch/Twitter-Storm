import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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
import backtype.storm.topology.base.BaseRichBolt;



class TweetTopology
{
	
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    //create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
    		"QTrVS64B4HU2TmNXAVwnZS9Bk",
    		"gmlPIJCWoIEuvB8sTX7VZcXaKxZOuStA7JGv05lOaldXcIJuNW",
    		"1469229614-EbAzs0awEM5d6cMmvs44l9DLTDTGyzhYE7NZY0q",
    		"fCv3a7fVrCVlk2ga8E5OQTVSLZvIuusKPkq4WMAFAFsYr");
        //"[Your customer key]",
        //"[Your secret key]",
        //"[Your access token]",
        //"[Your access secret]"    

    //****build the topology************
    
    //Twitter Spout
    builder.setSpout("tweet-spout", tweetSpout, 1);

    // Twitter Spout ===>> Category Bolt
    builder.setBolt("category-bolt", new CategoryBolt()).shuffleGrouping("tweet-spout");
    //  Category Bolt ===>> Tweet Printer
    builder.setBolt("print-tweets", new TweetPrinterBolt()).shuffleGrouping("category-bolt");
    
    //code to include the stright counting bolt - uncomment to use
    //builder.setBolt("count-bolt", new CountBolt()).fieldsGrouping("category-bolt", new Fields("category"));
    //builder.setBolt("print-count", new CountPrinterBolt("counts-print")).shuffleGrouping("count-bolt");
    
    //Category Bolt ===>> Rolling Counter Bolt  **uses a fields grouping**
    builder.setBolt("rolling-count-bolt", new RollingCountBolt(3600,1800)).fieldsGrouping("category-bolt", new Fields("category"));
    //Rolling Counter Bolt ===>> Count Printer
    builder.setBolt("print-count", new CountPrinterBolt("rolling-counts-print")).shuffleGrouping("rolling-count-bolt");
    
    
    // Submit and run the topology
    
    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

      // run it in a simulated local cluster
      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-flu-count", conf, builder.createTopology());

      // let the topology run for 5 hours. note topologies never terminate!
      Utils.sleep(1800000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();    
  }//end of main
}//end of class
