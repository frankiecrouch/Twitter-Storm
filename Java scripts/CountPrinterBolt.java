public static class CountPrinterBolt extends BaseBasicBolt {
	
	private final String filename;
	
	public CountPrinterBolt(String filename){
		this.filename = filename;
	}	
	
	     @Override
	     public void execute(Tuple tuple, BasicOutputCollector collector) {
	   
			try {
			BufferedWriter output;
			output = new BufferedWriter(
				new FileWriter("/home/ubuntu/workspace/Stage6/"+ filename +".txt", true));
			
			output.newLine();

			int windowLenght = tuple.getInteger(2); //gets the window lenght
			double mins = (double)(windowLenght+1)/60; //converts to minutes
			DecimalFormat df = new DecimalFormat("0"); //formatting
			//print the window			
			output.append("Count for the past " + df.format(mins) + " mintues ");

			//get the current time
			DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyy HH:mm:ss");
			Calendar cal = Calendar.getInstance();//formatting

			//print date and time
			output.append("at" + dateFormat.format(cal.getTime()) + ": ");
			//print the category
			output.append(tuple.getString(0) + " = ");
			//print the count
			output.append(Integer.toString(tuple.getInteger(1)));

			output.close();
			} catch (IOException e) { e.printStackTrace();}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer ofd) {
		}
}//end of class