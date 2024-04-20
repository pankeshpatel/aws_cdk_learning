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
        self.energystate_table = timestream.CfnTable(
            self, "energyStateTable",
            database_name=self.timestream_database.database_name,
            table_name="energyStateTable"
        )

        self.lightstate_table = timestream.CfnTable(
            self, "lightStateTable",
            database_name=self.timestream_database.database_name,
            table_name="lightStateTable"
        )

        # Ensure the table creation waits for the database creation
        self.energystate_table.add_depends_on(self.timestream_database)
        self.lightstate_table.add_depends_on(self.timestream_database)

        # Lambda function to process energy state data
        self.upstream_lambda = lambda_.Function(
            self, "upstream_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="upstream_lambda.handler",
            code=lambda_.Code.from_asset("lambda"),
            environment=dict(
                TIMESTREAM_DATABASE_NAME=self.timestream_database.database_name,
                TIMESTREAM_ENERGYSTATE_TABLE_NAME=self.energystate_table.table_name,
                TIMESTREAM_LIGHTSTATE_TABLE_NAME=self.lightstate_table.table_name
            ),
            function_name=f"upstream_lambda",
        )
    
        # IoT rule that triggers the energystate lambda function
        # electric-outdoors/iot/canopy1/upstream/energy-state
        self.upstream_rule = iot.CfnTopicRule(
            self, "upstream_rule",
            rule_name="upstream_rule",  # Required
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT *, topic() as topic FROM 'electric-outdoors/iot/+/upstream/+'",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.upstream_lambda.function_arn
                        )
                    )
                ]
            )
        )

        # Grant the IoT service permission to invoke the EnergyStateLambda  function
        self.upstream_lambda.add_permission(
            "AllowToInvoke",
            principal= aws_iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=self.upstream_rule.attr_arn
        )

        # # Grant the IoT service permission to invoke the Lambda function
        # self.lambda_function.add_permission(
        #     "AllowToInvoke",
        #     principal= aws_iam.ServicePrincipal("iot.amazonaws.com"),
        #     action="lambda:InvokeFunction",
        #     source_arn=self.topic_rule.attr_arn
        # )
        

        self.upstream_lambda.role.add_to_policy(aws_iam.PolicyStatement(
            actions=["timestream:WriteRecords", "timestream:DescribeEndpoints"],
            resources=["*"]
        ))