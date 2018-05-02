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


public class AmazonFoodAnalyticMapperOne extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable valore = new IntWritable(1);
    private Text chiave = new Text();

    private static final int TIME = 7;
    private static final int SUMMARY = 8;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapperOne.class);

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
            time = Long.parseLong(campi[TIME]);
        } catch (NumberFormatException e) {
            return;
        }

        Date date = Date.from(Instant.ofEpochSecond(time));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);

        StringTokenizer tokenizer = new StringTokenizer(campi[SUMMARY], " \t\n\r\f,.:;?![]'");

        while (tokenizer.hasMoreTokens()) {
            this.chiave.set(String.valueOf(year) + "|" + tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", ""));
            context.write(this.chiave, this.valore);

            LOG.info("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());
        }


    }
}
