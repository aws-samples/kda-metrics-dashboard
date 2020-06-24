/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.operators.CountWindowFn;
import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.utils.EmployeeInfoDeserializationSchema;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class DemoKDAApp {

    private static final Logger LOG = LoggerFactory.getLogger(DemoKDAApp.class);

    private static final Long MAX_OUT_OF_ORDERNESS_IN_MS = 2000L;
    private static final Long TUMBLING_WINDOW_PERIOD_IN_MS = 2000L;

    public static DataStream<EmployeeInfo> createKinesisSource(StreamExecutionEnvironment env,
                                                               ParameterTool parameter) throws Exception {

        //set Kinesis consumer properties
        Properties kinesisConsumerConfig = new Properties();
        //set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
                parameter.get("AWS_REGION", "us-east-2"));
        //obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

        String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

        if (adaptiveReadSettingStr.equals("true")) {
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
        } else {
            //poll new events from the Kinesis stream once every second
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                    parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
            // max records to get in shot
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
                    parameter.get("SHARD_GETRECORDS_MAX", "10000"));
        }


        //create Kinesis source
        DataStream<EmployeeInfo> kinesisStream = env.addSource(new FlinkKinesisConsumer<EmployeeInfo>(
                //read events from the Kinesis stream passed in as a parameter
                parameter.get("KINESIS_STREAM", "AmazonKinesisStream1"),
                //deserialize events with EventSchema
                new EmployeeInfoDeserializationSchema(),
                //using the previously defined properties
                kinesisConsumerConfig
        )).name("KinesisSource");

        return kinesisStream;
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure for event time (as opposed to processing time)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool parameter;

        // get application parameters
        if (env instanceof LocalStreamEnvironment) {
            //read the parameters specified from the command line
            parameter = ParameterTool.fromArgs(args);
        } else {
            //read the parameters from the Kinesis Analytics environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.");
            }

            parameter = ParameterToolUtils.fromApplicationProperties(flinkProperties);
        }

        // read from Kinesis
        DataStream<EmployeeInfo> rawEmployeeStream = createKinesisSource(env, parameter);

        //rawEmployeeStream.print();

        // assign timestamps and watermarks
        DataStream<EmployeeInfo> employeeStream =
                rawEmployeeStream.assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<EmployeeInfo>(
                        		Time.milliseconds(MAX_OUT_OF_ORDERNESS_IN_MS)) {
                            @Override
                            public long extractTimestamp(EmployeeInfo employeeInfo) {
                                return employeeInfo.getEventtimestamp();
                            }
                        });

        // do a simple aggregation that returns number of employees (f1) per company (f0) per window
		SingleOutputStreamOperator<Tuple2<Long, Long>> procStream = employeeStream.keyBy("companyid")
				.timeWindow(Time.milliseconds(TUMBLING_WINDOW_PERIOD_IN_MS))
				.apply(new CountWindowFn());

		procStream.print();

        // execute program
        env.execute("Demo KDA Kinesis Consumer");
    }
}