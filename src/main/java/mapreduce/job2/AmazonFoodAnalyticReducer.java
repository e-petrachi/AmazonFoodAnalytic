package mapreduce.job2;

import mapreduce.AmazonFoodConstants;
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
        Reducer<Text, Text, Text, Text> {

    private Text result;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducer.class);
    static { LOG.setLevel(Level.DEBUG);}

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        HashMap<Integer, ArrayList<Integer>> year2score = this.createHashYears(values);

        this.result = this.getResult(year2score);

        context.write(key, this.result);

        LOG.debug("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + this.result);
    }

    protected HashMap<Integer, ArrayList<Integer>> createHashYears(Iterable<Text> values){
        HashMap<Integer, ArrayList<Integer>> year2score = new HashMap<>();

        for (Text value : values) {
            int anno = Integer.parseInt(value.toString().substring(0,4));
            if (anno >= AmazonFoodConstants.YEARFROM && anno <= AmazonFoodConstants.YEARTO) {
                int score = Integer.parseInt(value.toString().substring(5));
                ArrayList<Integer> scores = null;
                if (year2score.containsKey(anno)) {
                    scores = year2score.get(anno);
                    scores.add(score);
                } else {
                    scores = new ArrayList<>();
                    scores.add(score);
                }
                year2score.put(anno, scores);
            }
        }

        return year2score;
    }

    protected Text getResult(HashMap<Integer, ArrayList<Integer>> year2score){
        String s = "";
        for (Integer year : year2score.keySet()){
            int sum = 0;
            for (Integer score: year2score.get(year)){
                sum+=score;
            }
            s = s + year + ":" + sum/year2score.get(year).size() + " ";
        }
        return new Text(s);
    }
}
