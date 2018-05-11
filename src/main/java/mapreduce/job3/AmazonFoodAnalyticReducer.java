package mapreduce.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;


public class AmazonFoodAnalyticReducer extends
        Reducer<Text, Text, Text, Text> {

    private Text result;
    private Text chiave;

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalyticReducer.class);
    static { LOG.setLevel(Level.INFO);}

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        TreeSet<String> users_ids = this.createUsersSet(values);


        ArrayList<String> users_list = new ArrayList<>(users_ids);

        while(users_list.size() > 1){
            for (int i = 1; i < users_list.size()-1; i++) {
                this.chiave = new Text(users_list.get(0));
                this.result = new Text(users_list.get(i));

                context.write(this.chiave, this.result);

                LOG.debug("* REDUCER_KEY: " + chiave + " * REDUCER_VALUE: " + result);
            }
            users_list.remove(0);
        }
    }
    protected TreeSet<String> createUsersSet(Iterable<Text> values){
        TreeSet<String> users_ids = new TreeSet<>();
        for (Text value: values){
            String user_id = value.toString();
            users_ids.add(user_id);
        }
        return users_ids;
    }
}
