from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_timestream as timestream
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_iot as iot
from aws_cdk import aws_iam
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_s3 as s3

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

        # Lambda function to process energy state data
        self.ingestion_lambda = lambda_.Function(
            self, "ingestion_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="ingestion_lambda.handler",
            code=lambda_.Code.from_asset("lambda"),
            environment=dict(
                TIMESTREAM_DATABASE_NAME=self.timestream_database.database_name,
                TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME=self.electric_outdoor_table.table_name,
            ),
            function_name=f"ingestion_lambda"
        )

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

        # Create IAM role for IoT actions to Firehose
        iot_to_firehose_role = aws_iam.Role(
            self, "IoTToFirehoseRole",
            role_name="IoTToFirehoseRole",
            assumed_by=aws_iam.ServicePrincipal("iot.amazonaws.com")
        )

        # Create and attach an inline policy to the role
        firehose_policy_statement = aws_iam.PolicyStatement(
            actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
            resources=[self.firehose_stream.attr_arn],
            effect=aws_iam.Effect.ALLOW
        )
       
        # Add the policy statement to the role
        iot_to_firehose_role.add_to_policy(firehose_policy_statement)

        # Delayed IoT rule creation to ensure role propagation
        self.electric_outdoor_ingestion_rule = iot.CfnTopicRule(
            self, "electric_outdoor_ingestion_rule",
            rule_name="electric_outdoor_ingestion_rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT *, topic(3) as canopy_id, topic(5) as message_type FROM 'electric-outdoors/iot/+/upstream/+'",
                aws_iot_sql_version="2016-03-23",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.ingestion_lambda.function_arn
                        )
                    ),
                    iot.CfnTopicRule.ActionProperty(
                        firehose=iot.CfnTopicRule.FirehoseActionProperty(
                            delivery_stream_name=self.firehose_stream.delivery_stream_name,
                            role_arn=iot_to_firehose_role.role_arn,
                            separator="\n"
                        )
                    )
                ]
            )
        )

        # Grant the IoT service permission to invoke the Lambda function
        self.ingestion_lambda.add_permission(
            "AllowToInvoke",
            principal= aws_iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=self.electric_outdoor_ingestion_rule.attr_arn
        )

        # Permission for Lambda to write to Timestream
        self.ingestion_lambda.role.add_to_policy(aws_iam.PolicyStatement(
            actions=["timestream:WriteRecords", "timestream:DescribeEndpoints"],
            resources=["*"]
        ))
