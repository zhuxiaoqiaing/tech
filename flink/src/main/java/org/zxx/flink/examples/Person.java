package org.zxx.flink.examples;
import java.io.Serializable;
/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/28 16:19
 */
public class Person implements Serializable {
    public String name;
    public int score;
    Person(String name,int score){
        this.name=name;
        this.score=score;
    }
}
