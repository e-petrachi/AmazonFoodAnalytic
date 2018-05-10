package mrspark.job3;

import mapreduce.AmazonFoodConstants;
import mrspark.ReviewsConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Objects;

public class AmazonFoodAnalytic {

    private static final Logger LOG = Logger.getLogger(AmazonFoodAnalytic.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("mrspark.job3.AmazonFoodAnalytic");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // args[0] = path and namefile.csv
        JavaRDD<String> csvdata = sc.textFile(args[0]);

        JavaRDD<ReviewsConstants> data = csvdata.map( csvline -> {
            String[] fields = csvline.split(",");

            return new ReviewsConstants(fields[AmazonFoodConstants.PRODUCTID],fields[AmazonFoodConstants.USERID]);
        }).filter(Objects::nonNull);


        JavaPairRDD<String, String> user2id = data
                .mapToPair( rc -> new Tuple2<>(rc.getUSERID(),rc.getPRODUCTID())).distinct();

        JavaPairRDD<String, Tuple2<String,String>> user2id2id = user2id.join(user2id);

        JavaPairRDD<Tuple2<String,String>, Integer> id2id2occ = user2id2id
                .mapToPair( x -> new Tuple2<>(x._2(),1)).reduceByKey((x,y) -> x + y);

        // rimuovi doppioni di coppie inverse


        // args[1] = path and namefile
        id2id2occ.coalesce(1).saveAsTextFile(args[1]);


        LOG.info("\n\n\n\n\n\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds\n\n\n\n\n\n");

        sc.stop();

    }


}
