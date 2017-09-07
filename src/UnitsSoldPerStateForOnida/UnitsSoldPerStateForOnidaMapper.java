/*
 * UnitsSoldPerStateForOnidaMapper is a mapper class which takes input as TextInputFormat whose input key is ByteOffset and input value is entire line
 * and writes state as key and 1 as value for Onida comapny
 */
package UnitsSoldPerStateForOnida;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
 * Extending Mapper which Maps input key/value pairs to a set of intermediate key/value pairs.
 * 
 */
public class UnitsSoldPerStateForOnidaMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	@Override
	public void map(LongWritable inputKey, Text inputValue,Context context) throws IOException, InterruptedException{
   
		//Splitting the line using separator |
		String[] lineArray= inputValue.toString().split("\\|"); 
		
		//ComapnyName is in first column of a record so storing from lineArray[0]
		Text companyName = new Text(lineArray[0]); 
		
		//state is in fourth column of a record so storing from lineArray[3]
		Text state = new Text(lineArray[3]);
   
		/* 
		 * Context is used for write operation in map reduce program, here companyName will be send as key and state will be send as value  which will be grouped in shuffle and sort phase of reducer 
		 */
		if((isBadRecord(inputValue)==false ) && companyName.toString().trim().equals("Onida") )//if comapny name is Onida and does not contain bad records then write
		context.write(state, new IntWritable(1));
   }
	
	private boolean isBadRecord(Text record){
	    String[] lineArray= record.toString().split("\\|");
	    System.out.println(lineArray[0]);//Storing comapnyName
	    System.out.println(lineArray[1]);//Storing product name
	    if ( (lineArray[0].equals("NA")) || (lineArray[1].equals("NA")) )//check if Company name and product name contains bad records "NA"
	        return true;
	    else
	        return false;//returns false if does not contain bad records
	    
	}
   
  }

