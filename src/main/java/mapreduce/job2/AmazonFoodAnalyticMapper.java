package mapreduce.job2;

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

    private Text valore;
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticMapper.class);
    static { LOG.setLevel(Level.DEBUG);}

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] campi = null;
        try {
            campi = new CSVParser().parseLine(line);
        } catch (IOException e){
            return;
        }

        String id = campi[AmazonFoodConstants.PRODUCTID];

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

        int score = 0;
        try {
            score = Integer.parseInt(campi[AmazonFoodConstants.SCORE]);
        } catch (NumberFormatException e) {
            return;
        }

        this.chiave = new Text(id);
        this.valore = new Text(year + "|" + score);

        context.write(this.chiave, this.valore);

        LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + this.valore.toString());
    }
}
