## KDA Metrics Dashboard - CloudFormation Template

This repository contains a CloudFormation template that you can customize to deploy a sample metrics dashboard for your [Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html) application.

Here's a screenshot of the dashboard:

![Dashboard screenshot](images/metrics_ss.png)

Before you deploy the template, please make sure that you enter appropriate values for these parameters:

- ApplicationName: The KDA app to monitor
- KDARegion: The region where the KDA app is deployed
- KinesisStreamName: The Kinesis stream name which is configured as input in Kinesis Analytics application (assuming you're reading from a Kinesis stream)
- KafkaTopicName: The Kafka topic name which is configured as input in Kinesis Analytics application (assuming you're reading from a Kafka topic)
- DashboardName: The name you'd like to assign to the newly created CloudWatch Dashboard

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This sample is licensed under the MIT-0 License. See the LICENSE file.

