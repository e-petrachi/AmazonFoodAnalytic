import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;


public class AmazonFoodAnalyticMapperTwo extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable valore = new IntWritable(1);
    private Text chiave = new Text();


    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapperTwo.class);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String year = line.substring(0,4);
        String word = line.substring(5);

        LOG.info("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());

    }
}
