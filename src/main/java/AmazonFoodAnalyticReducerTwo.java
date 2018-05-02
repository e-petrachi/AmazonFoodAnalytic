import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;


public class AmazonFoodAnalyticReducerTwo extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducerTwo.class);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));

        LOG.info("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + sum);
    }
}
