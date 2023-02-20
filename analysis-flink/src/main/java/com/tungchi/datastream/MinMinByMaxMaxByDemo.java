package com.tungchi.datastream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yingkf
 * @date 2023年02月20日11:21:16
 */
public class MinMinByMaxMaxByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(0, 5, 1));
        data.add(new Tuple3<>(0, 6, 8));
        data.add(new Tuple3<>(1, 1, 5));
        data.add(new Tuple3<>(1, 3, 3));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 4, 7));
        DataStream<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(data);
        minTest(source, env);
        // minByTest(source, env);
        // maxTest(source, env);
        // maxByTest(source, env);
    }

    private static void minTest(DataStream<Tuple3<Integer, Integer, Integer>> source,
            StreamExecutionEnvironment env) throws Exception {
        source.keyBy(v -> v.f0).min(2).print();
        env.execute("测试min");
    }

    private static void minByTest(DataStream<Tuple3<Integer, Integer, Integer>> source,
            StreamExecutionEnvironment env) throws Exception {
        source.keyBy(v -> v.f0).minBy(2).print();
        env.execute("测试minBy");
    }

    private static void maxTest(DataStream<Tuple3<Integer, Integer, Integer>> source,
            StreamExecutionEnvironment env) throws Exception {
        source.keyBy(v -> v.f0).max(2).print();
        env.execute("测试max");
    }

    private static void maxByTest(DataStream<Tuple3<Integer, Integer, Integer>> source,
            StreamExecutionEnvironment env) throws Exception {
        source.keyBy(v -> v.f0).maxBy(2).print();
        env.execute("测试maxBy");
    }
}
