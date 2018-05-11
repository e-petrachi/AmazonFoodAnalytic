package mapreduce.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;


public class AmazonFoodAnalyticReducerTwo extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducerTwo.class);
    static { LOG.setLevel(Level.INFO);}

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        ArrayList<Boolean> vv = new ArrayList<>();
        values.forEach( x-> vv.add(Boolean.TRUE));

        this.result = new IntWritable(vv.size());

        context.write(key , this.result);

        LOG.debug("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + result);
    }
}
