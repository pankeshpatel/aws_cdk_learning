from aws_cdk import (
    Stack
)
from constructs import Construct
from aws_cdk import aws_timestream as timestream
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_iot as iot
from aws_cdk import aws_iam

class TimeStreamSdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # Create a Timestream database
        self.timestream_database = timestream.CfnDatabase(
            self, "canopyData",
            database_name="canopyData")

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
            function_name=f"ingestion_lambda",
        )
    
        # IoT rule that triggers the energystate lambda function
        # electric-outdoors/iot/canopy1/upstream/energy-state
        self.electric_outdoor_ingestion_rule = iot.CfnTopicRule(
            self, "electric_outdoor_ingestion_rule",
            rule_name="electric_outdoor_ingestion_rule",  # Required
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT *, topic(3) as canopy_id, topic(5) as message_type FROM 'electric-outdoors/iot/+/upstream/+'",
                aws_iot_sql_version="2016-03-23",  # Specify the SQL version here
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.ingestion_lambda.function_arn
                        )
                    )
                ]
            )
        )


        # Grant the IoT service permission to invoke the EnergyStateLambda  function
        self.ingestion_lambda.add_permission(
            "AllowToInvoke",
            principal= aws_iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=self.electric_outdoor_ingestion_rule.attr_arn
        )

        self.ingestion_lambda.role.add_to_policy(aws_iam.PolicyStatement(
            actions=["timestream:WriteRecords", "timestream:DescribeEndpoints"],
            resources=["*"]
        ))