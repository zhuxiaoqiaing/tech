package org.zxx.spark.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/27 17:03
 */
public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {

        SparkConf config=new SparkConf().setAppName("mytest");
        JavaSparkContext spark = new JavaSparkContext(config);
        JavaRDD<String> lines=spark.parallelize(Arrays.asList("i am a good boy", "i am a good girls"),2);//Arrays.asList("abc,1", "test,2")
        JavaRDD<String> words=lines.flatMap(t->Arrays.asList(SPACE.split(t)).iterator());
        JavaPairRDD<String,Integer> word_map=words.mapToPair(t->new Tuple2<>(t,1));
        JavaPairRDD<String,Integer> word_count=word_map.reduceByKey((t1,t2)->(t1+t2));
        long results=word_count.count();
        System.out.println("result:"+results);
        spark.stop();
    }
}
