from aws_cdk import (
    Stack
)
from constructs import Construct
from aws_cdk import aws_timestream as timestream
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_iot as iot
from aws_cdk import aws_iam
from aws_cdk import aws_kms


class TimeStreamSdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

       # Create a Timestream database
        self.timestream_database = timestream.CfnDatabase(
            self, "MyTimestreamDatabase",
            database_name="MyDatabase")

        # Create a Timestream table within the database
        self.timestream_table = timestream.CfnTable(
            self, "MyTimestreamTable",
            database_name=self.timestream_database.database_name,
            table_name="MyTable",
            retention_properties={
                "MemoryStoreRetentionPeriodInHours": "24",
                "MagneticStoreRetentionPeriodInDays": "7"
            }
        )

         # Ensure the table creation waits for the database creation
        self.timestream_table.add_depends_on(self.timestream_database)

        # Lambda function to process IoT messages
        self.lambda_function = lambda_.Function(
            self, "IoTLambdaHandler",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="iot_lambda.handler",  # Assumes a file iot_lambda.py with a function handler
            code=lambda_.Code.from_asset("lambda"),
            environment=dict(
                TIMESTREAM_DATABASE_NAME=self.timestream_database.database_name,  # Required
                TIMESTREAM_TABLE_NAME=self.timestream_table.table_name,  # Required
            )
        )

        # IoT rule that triggers the Lambda function
        self.topic_rule = iot.CfnTopicRule(
            self, "IoTTopicRule",
            rule_name="IoTDataToTimestream",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT * FROM 'iot/topic/test'",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.lambda_function.function_arn
                        )
                    )
                ]
            )
        )

       # Grant the Lambda function permissions to write to Timestream
        # Add permission to the Lambda's role to access Timestream
        self.lambda_function.role.add_to_policy(aws_iam.PolicyStatement(
            actions=["timestream:WriteRecords", "timestream:DescribeEndpoints"],
            resources=["*"]  # Ideally, specify more restricted resources if possible
        ))

        # Grant the IoT service permission to invoke the Lambda function
        self.lambda_function.add_permission(
            "AllowIoTInvoke",
            principal= aws_iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=self.topic_rule.attr_arn
        )