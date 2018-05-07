package spark.job1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.ReviewsConstants;

import java.time.Instant;
import java.util.*;

public class AmazonFoodAnalytic {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark.job1.AmazonFoodAnalytic");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // args[0] = path and namefile.csv
        JavaRDD<String> csvdata = sc.textFile(args[0]);
        JavaRDD<ReviewsConstants> data = sc.textFile(String.valueOf(csvdata)).map(
                new Function<String, ReviewsConstants>() {
                    @Override
                    public ReviewsConstants call(String line) throws Exception {
                        String[] fields = line.split(",");
                        // potrebbero servirci costruttori diversi per ogni job
                        ReviewsConstants rc = new ReviewsConstants(Integer.parseInt(fields[0]),fields[1],fields[2],
                                Integer.parseInt(fields[6]), Integer.parseInt(fields[7]),fields[8]);
                        return rc;
                    }
                }
        );
        JavaPairRDD<Integer, String> years2word = map4years(data);
        JavaPairRDD<Integer, String> years2totalocc = reduceBYwords(years2word);



    }
    private static JavaPairRDD<Integer, String> map4years(JavaRDD<ReviewsConstants> data) {
        JavaPairRDD<Integer, String> couples =
                data.flatMapToPair(
                        new PairFlatMapFunction<ReviewsConstants, Integer, String>() {
                            @Override
                            public Iterator<Tuple2<Integer, String>> call(ReviewsConstants reviewsConstants) throws Exception {
                                StringTokenizer tokenizer = new StringTokenizer(reviewsConstants.getSUMMARY(), " \t\n\r\f,.:;?![]'");
                                ArrayList<Tuple2<Integer,String>> list2tuples = new ArrayList<>();
                                while (tokenizer.hasMoreTokens()){
                                    list2tuples.add(new Tuple2<Integer,String>(reviewsConstants.getYEAR(),tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")));
                                }
                                return list2tuples.iterator();
                            }
                        }

                );

        return couples;
    }

    private static JavaPairRDD<Tuple2<Integer,String>, Integer> map4years_word(JavaRDD<ReviewsConstants> data) {
        JavaPairRDD<Tuple2<Integer,String>, Integer> couples =
                data.flatMapToPair(
                        new PairFlatMapFunction<ReviewsConstants, Tuple2<Integer,String>, Integer>() {
                            @Override
                            public Iterator<Tuple2<Tuple2<Integer,String>, Integer>> call(ReviewsConstants reviewsConstants) throws Exception {
                                StringTokenizer tokenizer = new StringTokenizer(reviewsConstants.getSUMMARY(), " \t\n\r\f,.:;?![]'");
                                ArrayList<Tuple2<Tuple2<Integer,String>, Integer>> list2tuples = new ArrayList<>();
                                while (tokenizer.hasMoreTokens()){
                                    list2tuples.add(new Tuple2<Tuple2<Integer,String>, Integer>(new Tuple2<Integer, String>(
                                            reviewsConstants.getYEAR(), tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "")),1));
                                }
                                return list2tuples.iterator();
                            }
                        }

                );

        return couples;
    }

    private static JavaPairRDD<Tuple2<Integer,String>, Integer> reduce4words(JavaPairRDD<Tuple2<Integer,String>, Integer> years2word2occ) {
        return years2word2occ.reduceByKey((a, b) -> a + b );
    }

    private static JavaPairRDD<Integer, String> reduceBYwords(JavaPairRDD<Integer, String> years2word){
        HashMap<Tuple2<Integer,String>, Integer> word2count = new HashMap<>();

        years2word.foreach( y -> {
            Tuple2<Integer, String> tuple2 = (Tuple2<Integer, String>) new Tuple2<>(y._1(),y._2());

            if (word2count.containsKey(tuple2)){
                int count = word2count.get(tuple2);
                count++;
                word2count.put(tuple2, count);
            } else {
                word2count.put(tuple2, new Integer(1));
            }
        });

        int i = 100;
        for (Tuple2<Integer,String> t : word2count.keySet()){
            if (i > 0) {
                System.out.println(t.toString());
                i--;
            }else
                break;

        }

        return null;
    }
}
