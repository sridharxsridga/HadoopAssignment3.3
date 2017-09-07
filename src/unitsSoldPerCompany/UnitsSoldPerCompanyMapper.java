/*
 * UnitsSoldPerCompanyMapper is a mapper class which takes input as TextInputFormat whose input key is ByteOffset and input value is entire line
 * and writes comapnyName as key and 1 as value
 */

package unitsSoldPerCompany;

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
public class UnitsSoldPerCompanyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	/*
	 * Maps are the individual tasks which transform input records into a intermediate records
	 * It takes input key as byteOffset ,since the default input and output format is TextInputFormat and TextOutputFormat respectively
	 * It takes value as the entire line
	 * This is called for each key/value pair in the InputSplit
	 */
@Override
public void map(LongWritable inputKey, Text inputValue,Context context) throws IOException, InterruptedException{
	//Passing 1 as value for each company which will be grouped in shuffle and sort phase
	IntWritable value = new IntWritable(1);
	
	//Splitting the line using separator |
	String[] lineArray= inputValue.toString().split("\\|"); 
	
	//ComapnyName is in first column of a record so storing from lineArray[0]
	Text companyName = new Text(lineArray[0]); 
	
	/* 
	 * Context is used for write operation in map reduce program, here companyName will be send as key and 1 will be send as value  which will be grouped in shuffle and sort phase of reducer 
	 */
	if(isBadRecord(inputValue)==false)//if does not contain bad records then write
	context.write(companyName,value);
	
	
}
/*Method to check if Company name and product name contains bad records "NA",
 * Return true if contains bad records and false if does not contain bad records 
 */
private boolean isBadRecord(Text record){
    String[] lineArray= record.toString().split("\\|"); //Splitting the line using separator |
    System.out.println(lineArray[0]);//Storing comapnyName
    System.out.println(lineArray[1]);//Storing product name
    if ( (lineArray[0].equals("NA")) || (lineArray[1].equals("NA")) )//check if Company name and product name contains bad records "NA"
        return true;
    else
        return false;//returns false if does not contain bad records
    
}
}
