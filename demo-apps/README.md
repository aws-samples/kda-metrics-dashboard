## How to configure the  Kinesis Data Analytics for Apache Flink Demo Application

We've included sample Kinesis Analytics for Apache Flink applications (producer and consumer) that you can use to try out the metrics dashboard in this repo. Here's the high level flow:

demo-kinesis-driver (KDA Apache Flink) => Kinesis stream => demo-kda-app (KDA Apache Flink)

## Steps

1. Create Kinesis stream for use by the sample applications. Please refer to the [Getting Started](https://docs.aws.amazon.com/streams/latest/dev/getting-started.html) developer guide for information on how to setup the AWS CLI and on how to create a stream.
2. Deploy the Kinesis data driver Flink app (demo-kinesis-driver) to generate data into the above Kinesis stream. Please refer to the [Getting Started](https://docs.aws.amazon.com/kinesisanalytics/latest/java/getting-started.html) developer guide for information on setting up your AWS account, the AWS CLI, and on deploying an Apache Flink app to Kinesis Data Analytics.
3. Deploy the the Kinesis Data Analytics consumer app (demo-kda-app). This is the application that you'll configure the metrics dashboard to monitor.