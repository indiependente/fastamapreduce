package simple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastaReducer extends Reducer<LongWritable, Text, Text, Text> 
{

}
