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

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.sources.RandEmployeeInfoSource;
import com.amazonaws.services.kinesisanalytics.utils.AvroSerializationFn;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class DemoKinesisDriver {

	private static final Logger LOG = LoggerFactory.getLogger(DemoKinesisDriver.class);

	public static FlinkKinesisProducer<EmployeeInfo> createKinesisSink(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

		String awsRegion = parameter.get("AWS_REGION", "us-east-2");

		Properties producerConfig = new Properties();

		// Required configs
		producerConfig.put(AWSConfigConstants.AWS_REGION, awsRegion);

		FlinkKinesisProducer<EmployeeInfo> kinesis =
				new FlinkKinesisProducer<EmployeeInfo>(new AvroSerializationFn(), producerConfig);

		kinesis.setFailOnError(true);
		kinesis.setDefaultStream(parameter.get("KINESIS_STREAM", "AmazonKinesisStream1"));

		kinesis.setCustomPartitioner(new KinesisPartitioner<EmployeeInfo>() {
			@Override
			public String getPartitionId(EmployeeInfo s) {
				// we dont' care about shard affinity in this app
				return UUID.randomUUID().toString();
			}
		});

		return kinesis;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool parameter;

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

		// use mock source to generate data
		DataStream<EmployeeInfo> employeeStream = env.addSource(new RandEmployeeInfoSource());

		// create and get Kinesis sink
		FlinkKinesisProducer<EmployeeInfo> kinesisSink = createKinesisSink(env, parameter);

		employeeStream.addSink(kinesisSink);

		// execute program
		env.execute("Demo Kinesis Driver");
	}
}