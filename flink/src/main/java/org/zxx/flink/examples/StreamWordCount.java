package org.zxx.flink.examples;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import java.util.Properties;

/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/28 15:39
 * 消费kafka数据进行wordcount，并落到kafka中
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println(".............start....");
        SingleOutputStreamOperator<String> person_ds=env.addSource(
                new FlinkKafkaConsumer09<>
                        ("flink_mysql_person", new SimpleStringSchema(),
                                getProperties()).setStartFromEarliest()).name("source kafka").
                flatMap(new WordCountFormat())
                .setParallelism(1).keyBy(0).timeWindowAll(Time.seconds(4)).sum(1).
                        map(new MapFunction<Tuple2<String, Long>,String>() {
                            @Override
                            public String map(Tuple2<String, Long> value) throws Exception {
                                return value.f0+"->>>>"+String.valueOf(value.f1);
                            }}
                        );
        System.out.println("........all............");
        //person_ds.print();
        //person_ds.addSink(new MysqlSink());
        person_ds.addSink(new FlinkKafkaProducer09<String>
              ("localhost:9092","zxxtopic" ,
                    new SimpleStringSchema())).name("sink kafka");
        env.execute("flink kafka");
    }
    public static Properties getProperties(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","test3");
        properties.setProperty("auto.offset.reset","earliest");
        return properties;
    }
}
