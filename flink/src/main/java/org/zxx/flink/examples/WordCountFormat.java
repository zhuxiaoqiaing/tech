package org.zxx.flink.examples;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/28 16:16
 */
public class WordCountFormat implements FlatMapFunction<String, Tuple2<String,Long>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if(value.contains(" ")){
            for(String str:value.split(" ")){
                out.collect(new Tuple2(str,1L));
            }
        }
    }
}
