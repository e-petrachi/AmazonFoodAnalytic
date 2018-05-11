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
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;


public class AmazonFoodAnalyticMapper extends
        Mapper<LongWritable, Text, Text, Text> {
    // il mapper si legge il file con chiave LongWritable x forza !

    private Text valore;
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapper.class);
    static { LOG.setLevel(Level.INFO);}

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] campi = null;
        try {
            campi = new CSVParser().parseLine(line);
        } catch (IOException e){
            return;
        }

        String product_id = campi[AmazonFoodConstants.PRODUCTID];
        String user_id = campi[AmazonFoodConstants.USERID];

        this.chiave = new Text(user_id);
        this.valore = new Text(product_id);

        context.write(this.chiave, this.valore);

        LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());

    }
}
