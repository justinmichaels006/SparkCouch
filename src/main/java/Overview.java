import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;

import com.couchbase.spark.japi.CouchbaseSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;
import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;

public class Overview {

    public static void main(String[] args) throws IOException {

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(true)
                .dnsSrvEnabled(false)
                //.observeIntervalDelay()
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(env, "192.168.61.101");
        Bucket bucket = cluster.openBucket("testload");


        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("spark://Justins-MacBook-Pro-2.local:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // The Couchbase-Enabled spark context
        CouchbaseSparkContext csc = couchbaseContext(sc);



        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println("Dist:  " + distData);

        JavaRDD<String> lines = sc.textFile("/Users/justin/Documents/Demo/spark/spark-1.6.0-bin-hadoop2.6/data/mllib/sample_isotonic_regression_data.txt");
        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length();}
        });
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        System.out.println("Line Count:  " + totalLength);

        Random ran = new Random();
        int NUM_SAMPLES = ran.nextInt(10) + 20;

        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }
        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer i) {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

    }
}

