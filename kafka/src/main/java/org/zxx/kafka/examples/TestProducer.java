package org.zxx.kafka.examples;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/14 12:25
 */
public class TestProducer {
    //设置kafka的参数
    public static Properties getPropertiesForKafka(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    public static void main(String[] args) {
        final String TOPIC_NAME = "flink_mysql_person";
        Producer<String, String> producer = new KafkaProducer(getPropertiesForKafka());
        System.out.println("start producing topic....");
        int i = 0;
        while (i<100000) {
            //try {
              //  Thread.sleep(100);
            //}
            //catch(Exception e){
                //
            //}
            String []names={"i","am","a","student"};
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, i + "", names[i%4] + ""));
            i += 1;
        }
        producer.close();
    System.out.println("close the connnection.....");
    }

}
