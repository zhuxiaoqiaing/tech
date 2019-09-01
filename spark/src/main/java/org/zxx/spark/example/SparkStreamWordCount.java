package org.zxx.spark.example;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.io.File;
import java.io.FileWriter;
import java.util.*;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/27 20:00
 */
public class SparkStreamWordCount {
    static final String TOPIC_NAME="sparkstream_test";

    //spark stream 接入kafka流
    public static void main(String[] args)throws Exception {
        SparkConf conf = new SparkConf().setAppName("spark stream test");
        JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(5));
        Map<String,String> kafka_parms=new HashMap();
        kafka_parms.put("bootstrap.servers","192.168.1.101:9092");
        kafka_parms.put("group.id", "sparkstream_test");
        //kafka_parms.put("auto.offset.reset", "smallest");
        //kafka_parms.put("key.deserializer", StringDeserializer.class);
        //kafka_parms.put("value.deserializer", StringDeserializer.class);
        kafka_parms.put("auto.offset.reset", "largest");
        //kafka_parms.put("key.deserializer","");
        Set<String> topics=new HashSet<String>();
        topics.add(TOPIC_NAME);
        JavaPairInputDStream<String,String> ds = KafkaUtils.createDirectStream(jsc,String.class,String.class,StringDecoder.class,StringDecoder.class,kafka_parms,topics);
        /*
        JavaPairDStream javaPairDStream =ds.mapToPair(new PairFunction<ConsumerRecord<String,String>,String,String>() {
                    @Override
                    public Tuple2 call(ConsumerRecord<String,String> recorder) throws Exception {
                        return new Tuple2(recorder.key(), recorder.value();
                    }});
         */
          ds.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
             @Override
             public void call(JavaPairRDD<String,String> rdd) throws Exception {
                 rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                     @Override
                     public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                         StringBuilder sb=new StringBuilder();
                         int i=0;
                         while(tuple2Iterator.hasNext()){
                             String kvs=tuple2Iterator.next()._2;
                             sb.append(kvs);
                             if(i%10==0)
                                 sb.append("\n");
                             i+=1;
                         }
                         //写入外存
                         FileWriter fo=new FileWriter(new File("d:\\ab.txt"),true);
                         fo.write(sb.toString());
                         //关闭文件
                         fo.close();
                     }
                 });
             }
         });
          jsc.start();
          jsc.awaitTermination();
    }
}
