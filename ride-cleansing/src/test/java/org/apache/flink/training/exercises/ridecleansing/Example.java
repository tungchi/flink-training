package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yingkf
 * @date 2023年02月16日09:22:48
 */
public class Example {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Person> flintstones = env.fromElements(new Person("Fred", 35),
                new Person("Wilma", 35), new Person("Pebbles", 2));
        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });
        adults.print();
        env.execute();
    }

    public static class Person {
        private final String name;
        private final Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name + ": age " + this.age;
        }
    }
}
