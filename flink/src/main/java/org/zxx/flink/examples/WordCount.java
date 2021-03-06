package org.zxx.flink.examples;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/28 8:55
 */
public class WordCount {
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> ds = env.readTextFile(params.get("input"));
        DataSet<Tuple2<String,Integer>> res=ds.flatMap(new Tokenizer()).groupBy(0).sum(1);
        res.writeAsCsv(params.get("output"));
        env.execute("this is a good job");
    }
    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String [] vs=value.split(" ");
            for(String v:vs)
            {
                out.collect(new Tuple2<String,Integer>(v,1));
            }
        }
    }
}
