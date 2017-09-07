/*
 * UnitsSoldPerCompanyReducer is a reducer class whose input key is comapnyName and input value is 1
 * and writes comapnyName as key and total units sold as value
 */
package unitsSoldPerCompany;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//Extending Reducer which Reduces a set of intermediate values which share a key to a smaller set of values.

public class UnitsSoldPerCompanyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	/*
	 * reduce method is called for each <key, (collection of values)> in the sorted inputs.
	 * Here companyname is given as key and valuesSold as value which will be list of values stored from intermediate result of map function
	 * 
	 */
	@Override
public void reduce(Text companyName, Iterable<IntWritable> valuesSold,Context context) throws IOException, InterruptedException{
	
	int counter =0; //Counter to count the no of units sold which will be incremented for each iterations of valuesSold value which we get after shuffle and sort phase
		
	for(IntWritable values : valuesSold ){//Iterate valuesSold value  which we get after shuffle and sort phase 
		counter++;
	}
	/* The output of the reduce task is typically written to a RecordWriter
	 * Context is used for write operation in map reduce program, here companyName will be send as key and count of counter will be send as the total number of units sold for that comapny
	 */
	
	context.write(companyName, new IntWritable(counter));
}
	
	
}
