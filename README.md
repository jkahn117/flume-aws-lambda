# flume-aws-lambda

Flume sink for AWS Lambda

## Build and Install

```
$ mvn package shade:shade

$ cp target/flume-aws-lambda-{version}.jar FLUME_HOME_DIR/plugins.d/flume-aws-lambda/lib
```

## Configuration

Configure the Sink as follows:

```
a1.sinks.k1.type = com.jkahn.flume.sink.aws.lambda.LambdaSink
a1.sinks.k1.channel = c1
a1.sinks.k1.region = us-east-1
a1.sinks.k1.functionName = myFunctionName
a1.sinks.k1.accessKey = ...
a1.sinks.k1.secretKey = ...
```

### Options

| Name | Description | Default | Optional |
|:----:|:----------:|:-------:|:--------:|
| region | Region of the Lambda function | us-east-1 | N |
| functionName | Name of the Lambda function that will process messages | null | N |
| accessKey | Access key with permission to invoke Lambda function | null | Y |
| secretKey | Secret key associated with access key | null | Y |

### AWS Credentials

This sink will follow the credential chains as defined in [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html):

* Environment Variables
* Java System Properties
* Credential profiles file at the default location utilized by AWS SDKs and CLI
* Credentials delivered via the Amazon EC2 Container Service if configured and allowed
* Instance profile credentials delivered through Amazon EC2 metadata service


## References
* https://flume.apache.org/FlumeDeveloperGuide.html
* https://github.com/dpandya/flume-ng-aws-sqs-sink
* https://github.com/sharethrough/flume-kinesis
* https://howtoprogram.xyz/2016/09/09/junit-5-maven-example/
