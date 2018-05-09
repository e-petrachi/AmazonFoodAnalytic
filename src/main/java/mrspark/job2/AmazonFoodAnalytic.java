package mrspark.job2;

import mapreduce.AmazonFoodConstants;
import mrspark.ReviewsConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("mrspark.job1.AmazonFoodAnalytic");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // args[0] = path and namefile.csv
        JavaRDD<String> csvdata = sc.textFile(args[0]);

        JavaRDD<ReviewsConstants> data = csvdata.map((String csvline) -> {
            String[] fields = csvline.split(",");

            long time;
            try {
                time = Long.parseLong(fields[AmazonFoodConstants.TIME]);
            } catch (Exception e){
                return null;
            }
            return new ReviewsConstants(time,fields[AmazonFoodConstants.SUMMARY]);
        });


        LOG.debug("FINE!");

    }

    private static JavaPairRDD<Integer, String> map4years(JavaRDD<ReviewsConstants> data) {
        JavaPairRDD<Integer, String> couples = data.filter(Objects::nonNull).flatMapToPair((ReviewsConstants rc) -> {

            StringTokenizer tokenizer = new StringTokenizer(rc.getSUMMARY(), " \t\n\r\f,.:;?![]'");
            ArrayList<Tuple2<Integer, String>> list2tuples = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                list2tuples.add(new Tuple2<>(rc.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")));
            }
            return list2tuples.iterator();

        });
        return couples;
    }



    private static JavaPairRDD<Tuple2<Integer,String>, Integer> reduce4words(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ) {
        return years2word2occ.reduceByKey((a, b) -> a + b );
    }

    private static List<Tuple2<Integer, Tuple2<Integer,String>>> top10words(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2count) {

        List<Tuple2<Integer, Tuple2<Integer,String>>> top10 =
                years2word2count.mapToPair( y2w2c -> new Tuple2<>(y2w2c._2(), new Tuple2<>(y2w2c._1()._1(),y2w2c._1()._2())))
                        .sortByKey(false)
                        .take(10);

        return top10;

    }


}
