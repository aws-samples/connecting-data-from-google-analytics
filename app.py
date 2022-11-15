#!/usr/bin/env python3

from aws_cdk import App, Tags,Aspects

from lib.glue.glue_stack import GlueJobStack
from cdk_nag import AwsSolutionsChecks, NagSuppressions

app = App()
glue_job = GlueJobStack(app, "gcp-connector-stack", description="(SO9084) AWS Glue Connector Ingestion for GA4 Analytics v1.0.0")
Tags.of(glue_job).add("project", "gcp-connector-stack")


NagSuppressions.add_stack_suppressions(
    glue_job,
    [
        {
            "id": "AwsSolutions-IAM5",
            "reason": "S3Bucket Deployment contains a wildcard permissions have apply to bucket",
        },
        {
            "id": "AwsSolutions-IAM4",
            "reason": "AWS Managed IAM policies are used for AWSGlueServiceRole and AmazonEC2ContainerRegistryReadOnly been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies",
        },
        {
            "id": "AwsSolutions-S1",
            "reason": "S3 Access Logs are disabled for demo purposes.",
        }
    ],
)

Aspects.of(app).add(AwsSolutionsChecks())



app.synth()
