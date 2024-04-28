from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_timestream as timestream
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_iot as iot
from aws_cdk import aws_iam
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kinesis as kinesis


class TimeStreamSdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a Timestream database
        self.timestream_database = timestream.CfnDatabase(
            self, "CanopyData",
            database_name="CanopyData")

        # Create a Timestream table within the database
        self.electric_outdoor_table = timestream.CfnTable(
            self, "IngestionTable",
            database_name=self.timestream_database.database_name,
            table_name="IngestionTable"
        )

        # Ensure the table creation waits for the database creation
        self.electric_outdoor_table.add_depends_on(self.timestream_database)
    
        # S3 bucket for Firehose destination
        self.data_bucket = s3.Bucket(
            self, "IoTFirehoseDataBucket",
            bucket_name="iot-firehose-data-bucket"
        )

        # Firehose delivery stream
        self.firehose_stream = firehose.CfnDeliveryStream(
            self, "IoTFirehoseStream",
            delivery_stream_name="IoTFirehoseStream",
            delivery_stream_type="DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=self.data_bucket.bucket_arn,
                role_arn=aws_iam.Role(
                    self, "FirehoseS3Role",
                    assumed_by=aws_iam.ServicePrincipal("firehose.amazonaws.com"),
                    managed_policies=[
                        aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                    ]
                ).role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=10,
                    size_in_m_bs=5
                )
            )
        )

        # Create a Kinesis Data Stream
        self.kinesis_stream = kinesis.Stream(
            self, "IoTKinesisStream",
            stream_name="IoTKinesisStream",
            shard_count=1  # Specify the number of shards for the stream
        )

        # Create IAM role for IoT actions to Firehose
        self.iot_to_kinesis_firehose_role = aws_iam.Role(
            self, "IoTToKinesisFirehoseRole",
            role_name="IoTToKinesisFirehoseRole",
            assumed_by=aws_iam.ServicePrincipal("iot.amazonaws.com")
        )

        # Create and attach an inline policy to the role
        self.firehose_policy_statement = aws_iam.PolicyStatement(
            actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
            resources=[self.firehose_stream.attr_arn],
            effect=aws_iam.Effect.ALLOW
        )

        self.kinesis_policy_statement = aws_iam.PolicyStatement(
            actions=["kinesis:PutRecord", "kinesis:PutRecords"],
            resources=[self.kinesis_stream.stream_arn],
            effect=aws_iam.Effect.ALLOW
        )
       
        # Add the policy statement to the role
        self.iot_to_kinesis_firehose_role.add_to_policy(self.firehose_policy_statement)
        self.iot_to_kinesis_firehose_role.add_to_policy(self.kinesis_policy_statement)

        self.ingestion_lambda = lambda_.Function(
                self, "ingestion_lambda_handler",
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="ingestion_lambda.handler",
                code=lambda_.Code.from_asset("lambda"),
                environment=dict(
                    TIMESTREAM_DATABASE_NAME=self.timestream_database.database_name,
                    TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME=self.electric_outdoor_table.table_name,
                    KINESIS_STREAM_NAME=self.kinesis_stream.stream_name
                ),
                function_name=f"ingestion_lambda"
        )

        self.kinesis_stream.grant_read(self.ingestion_lambda)
         # Permission for Lambda to write to Timestream
        self.ingestion_lambda.role.add_to_policy(aws_iam.PolicyStatement(
            actions=["timestream:WriteRecords", "timestream:DescribeEndpoints"],
            resources=["*"]
        ))

        # Setting up Lambda as a trigger for the Kinesis stream
        lambda_.EventSourceMapping(
            self, "KinesisLambdaMapping",
            target=self.ingestion_lambda,
            event_source_arn=self.kinesis_stream.stream_arn,
            batch_size=100,  # Number of records to read at once from Kinesis stream
            starting_position=lambda_.StartingPosition.LATEST
        )

        # Delayed IoT rule creation to ensure role propagation
        self.electric_outdoor_ingestion_rule = iot.CfnTopicRule(
            self, "electric_outdoor_ingestion_rule",
            rule_name="electric_outdoor_ingestion_rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT *, topic(3) as canopy_id, topic(5) as message_type FROM 'electric-outdoors/iot/+/upstream/+'",
                aws_iot_sql_version="2016-03-23",
                actions=[
                    # iot.CfnTopicRule.ActionProperty(
                    #     lambda_=iot.CfnTopicRule.LambdaActionProperty(
                    #         function_arn=self.ingestion_lambda.function_arn
                    #     )
                    # ),
                    iot.CfnTopicRule.ActionProperty(
                        firehose=iot.CfnTopicRule.FirehoseActionProperty(
                            delivery_stream_name=self.firehose_stream.delivery_stream_name,
                            role_arn=self.iot_to_kinesis_firehose_role.role_arn,
                            separator="\n"
                        )
                    ),
                    iot.CfnTopicRule.ActionProperty(
                        kinesis=iot.CfnTopicRule.KinesisActionProperty(
                            stream_name=self.kinesis_stream.stream_name,
                            role_arn=self.iot_to_kinesis_firehose_role.role_arn,
                            # partition_key="partitionKey"  # Update or remove this depending on your partition key logic
                        )
                    )
                ]
            )
        )