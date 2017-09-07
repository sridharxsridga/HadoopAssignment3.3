/*
 * UnitsSoldPerStateForOnidaReducer is a reducer class whose input key is state and input value is 1
 * and writes state as key and total units sold per state as value
 */
package UnitsSoldPerStateForOnida;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Extending Reducer which Reduces a set of intermediate values which share a key to a smaller set of values.
public class UnitsSoldPerStateForOnidaReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	/*
	 * reduce method is called for each <key, (collection of values)> in the sorted inputs.
	 * Here state is given as key and unitsSold as value which will be list of values stored from intermediate result of map function
	 * 
	 */
	@Override
public void reduce(Text state, Iterable<IntWritable> unitsSold,Context context) throws IOException, InterruptedException{
	
	int counter =0; //Counter to count the no of units sold which will be incremented for each iterations of unitsSold value which we get after shuffle and sort phase
		
	for(IntWritable values : unitsSold ){//Iterate unitsSold value  which we get after shuffle and sort phase 
		counter++;
	}
	/* The output of the reduce task is typically written to a RecordWriter
	 * Context is used for write operation in map reduce program, here state will be send as key and count of counter will be send as the total number of units sold for each state
	 */
	context.write(state, new IntWritable(counter));
}
	
	
}

