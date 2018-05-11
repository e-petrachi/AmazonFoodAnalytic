package mapreduce.job3;

import com.opencsv.CSVParser;
import mapreduce.AmazonFoodConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;


public class AmazonFoodAnalyticMapperTwo extends
        Mapper<Text, Text, Text, IntWritable> {

    private IntWritable valore;
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapperTwo.class);
    static { LOG.setLevel(Level.DEBUG);}

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.chiave = new Text(key.toString() + " " + value.toString());
        this.valore = new IntWritable(1);

        context.write(this.chiave, this.valore);

        LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());

    }
}
