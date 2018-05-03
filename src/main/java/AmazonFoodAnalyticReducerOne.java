import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;


public class AmazonFoodAnalyticReducerOne extends
        Reducer<IntWritable, Text, IntWritable, Text> {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducerOne.class);
    static { LOG.setLevel(Level.INFO);}

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> word2count = this.createHashWords(values);

        TreeMap<Integer, ArrayList<String>> count2values = this.createInvertedIndex(word2count);

        ArrayList<String> topWords = this.createTopTenWords(count2values);

        Text result = this.getResults(topWords,word2count);

        context.write(key, result);

        LOG.debug("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + result);
    }

    protected HashMap<String, Integer> createHashWords(Iterable<Text> values){
        HashMap<String, Integer> word2count = new HashMap<>();

        for (Text value : values) {
            if (word2count.containsKey(value.toString())) {
                int count = word2count.get(value.toString());
                count++;
                word2count.put(value.toString(), count);
            } else {
                word2count.put(value.toString(), new Integer(1));
            }
        }

        return word2count;
    }
    protected TreeMap<Integer, ArrayList<String>> createInvertedIndex(HashMap<String, Integer> word2count) {
        TreeMap<Integer, ArrayList<String>> count2values = new TreeMap<>();

        for (String value : word2count.keySet()) {
            Integer c = word2count.get(value);
            ArrayList<String> v;
            if (count2values.containsKey(c)) {
                v = count2values.get(c);
                v.add(value);
            } else {
                v = new ArrayList<>();
                v.add(value);
            }
            count2values.put(c, v);
        }

        return count2values;
    }

    protected ArrayList<String> createTopTenWords(TreeMap<Integer, ArrayList<String>> count2values){
        int top = 10;
        ArrayList<String> topWords = new ArrayList<>();
        for (Integer k : count2values.descendingKeySet()) {
            if (top > 0) {
                ArrayList<String> words = count2values.get(k);
                if (words.size() < top) {
                    topWords.addAll(words);
                    top = top - words.size();
                } else {
                    int i = 0;
                    while (top > 0) {
                        topWords.add(words.get(i));
                        top--;
                        i++;
                    }
                }
            } else {
                break;
            }
        }

        return topWords;
    }

    protected Text getResults(ArrayList<String> topWords, HashMap<String, Integer> word2count){
        String r = "";
        for (String t : topWords) {
            r = r.concat(t + ":" + word2count.get(t) + " | ");
        }
        return new Text(r);
    }

}
