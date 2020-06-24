package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWindowFn implements WindowFunction<EmployeeInfo, Tuple2<Long, Long>, Tuple, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(CountWindowFn.class);

    @Override
    public void apply(Tuple tuple,
                      TimeWindow timeWindow,
                      Iterable<EmployeeInfo> iterable,
                      Collector<Tuple2<Long, Long>> collector) throws Exception {

        long count = Iterables.size(iterable);
        EmployeeInfo employeeInfo = Iterables.get(iterable, 0);

        collector.collect(new Tuple2<>(employeeInfo.getCompanyid(), count));
    }
}