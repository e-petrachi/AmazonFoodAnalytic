package mapreduce.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;


public class AmazonFoodAnalyticMapperTwo extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    // il mapper si legge il file con chiave LongWritable x forza !

    private IntWritable valore;
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapperTwo.class);
    static { LOG.setLevel(Level.INFO);}

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        this.chiave = value;
        this.valore = new IntWritable(1);

        context.write(this.chiave, this.valore);

        LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());

    }
}
