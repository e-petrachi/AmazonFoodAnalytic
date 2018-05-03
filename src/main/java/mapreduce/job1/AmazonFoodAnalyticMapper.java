package mapreduce.job1;

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
        Mapper<LongWritable, Text, IntWritable, Text> {

    private Text valore;
    private IntWritable chiave;

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


        long time = 0L;
        try {
            time = Long.parseLong(campi[AmazonFoodConstants.TIME]);
        } catch (NumberFormatException e) {
            return;
        }

        Date date = Date.from(Instant.ofEpochSecond(time));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);

        StringTokenizer tokenizer = new StringTokenizer(campi[AmazonFoodConstants.SUMMARY], " \t\n\r\f,.:;?![]'");

        while (tokenizer.hasMoreTokens()) {
            this.chiave = new IntWritable(year);
            this.valore = new Text(tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", ""));
            context.write(this.chiave, this.valore);

            LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());
        }


    }
}
