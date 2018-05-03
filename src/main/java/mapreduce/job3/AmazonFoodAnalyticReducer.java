package mapreduce.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;


public class AmazonFoodAnalyticReducer extends
        Reducer<IntWritable, Text, IntWritable, Text> {

    private Text result;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducer.class);
    static { LOG.setLevel(Level.INFO);}

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        context.write(key, result);

        LOG.debug("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + result);
    }


}
