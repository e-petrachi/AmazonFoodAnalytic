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
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducerTwo.class);
    static { LOG.setLevel(Level.DEBUG);}

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        ArrayList<IntWritable> vv = new ArrayList<>();
        values.forEach( x-> vv.add(x));

        this.chiave = key;
        this.result = new IntWritable(vv.size());

        context.write(this.chiave , this.result);

        LOG.debug("* REDUCER_KEY: " + chiave + " * REDUCER_VALUE: " + result);
    }
}
