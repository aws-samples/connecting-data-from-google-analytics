import json
from constructs import Construct
from aws_cdk import App, Stack,Duration, Stack, CfnOutput,Tags

from aws_cdk import(
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    aws_s3_deployment as s3_deploy,
    Duration,
    aws_glue as glue,
    aws_kms as kms,
    aws_s3_deployment as s3deploy,
    aws_secretsmanager as secretsmanager,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs
)
import aws_cdk.aws_glue_alpha as glue_alpha

from aws_cdk.aws_stepfunctions import (
    JsonPath
)
class GlueJobStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.data_bucket_name = self.node.try_get_context("data_bucket_name")
        self.dataset_id = self.node.try_get_context("dataset_id")
        self.parent_project = self.node.try_get_context("parent_project")
        self.connection_name = self.node.try_get_context("connection_name")
        self.filter = self.node.try_get_context("filter")
        self.job_script = self.node.try_get_context("job_script")
        self.schedule = self.node.try_get_context("schedule_daily_hour")
        self.glue_database_name = self.node.try_get_context("glue_database")
        self.glue_table_name = self.node.try_get_context("glue_table")
        self.job_name = self.node.try_get_context("job_name")
        self.timedelta_days = self.node.try_get_context("timedelta_days")
        self.classification = self.node.try_get_context("classification")
        
        self.add_buckets(self.data_bucket_name)
        self.add_secret()
        self.add_role()
        self.add_scripts()
        self.add_jobs()
        self.add_crawler()
        self.add_workflow()

    ##############################################################################
    # S3 Buckets
    ##############################################################################

    def add_buckets(self,data_bucket_name):
        self.data_bucket = s3.Bucket(
            self,
            "glue_data_bucket",
            bucket_name=f"{data_bucket_name}",
            removal_policy=RemovalPolicy.DESTROY,
            encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True
        )

        self.glue_asset_bucket = s3.Bucket(
            self,
            "glue_asset_bucket",
            bucket_name=f"aws-glue-assets-logs-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        CfnOutput(self, "glue-data-bucket-name", value=self.data_bucket.bucket_name)
        CfnOutput(self, "glue-asset-bucket-name", value=self.glue_asset_bucket.bucket_name)   
        # tag data to classification
        Tags.of(self.data_bucket).add("Classification", self.classification)   
        Tags.of(self.glue_asset_bucket).add("Classification", self.classification)   

    ##########################################################################################
    # Secret: note - manually need to update the secret value in Console with the GCP secret
    ##########################################################################################
    
    def add_secret(self):
        # Check if the secrets exist and only create if it does not exit.  
        # Secret credentials needs to be added manually
        
        if (secretsmanager.Secret.from_secret_name_v2(self, "bigquery_credentials_from_name", "bigquery_credentials")):
            self.gcp_secret = secretsmanager.Secret.from_secret_name_v2(self, "bigquery_credentials", "bigquery_credentials")
        
        else:
            self.gcp_secret = secretsmanager.Secret(self, "bigquery_credentials",
                                                            secret_name="bigquery_credentials",
                                                            description="bigquery access - base64 service account credentials JSON file from GCP",
                                                            removal_policy=RemovalPolicy.RETAIN,
                                                            generate_secret_string=secretsmanager.SecretStringGenerator(
                                                                secret_string_template=json.dumps(
                                                                    {"credentials": ""}),
                                                                generate_string_key=""
                                                            )
                                                            )
    ##############################################################################
    # GlueJob
    ##############################################################################

    def add_jobs(self):
        
        self.kms_job = kms.Key(self, id='cmk',
                               alias=f"{self.job_name}_kms",
                               enable_key_rotation=True,
                               pending_window=Duration.days(7),
                               removal_policy=RemovalPolicy.DESTROY
                               )     
        
        self.kms_job.grant_encrypt_decrypt(self.role)
        self.service_cloudwatch = iam.ServicePrincipal(service='logs.amazonaws.com')
        self.kms_job.grant_encrypt_decrypt(self.service_cloudwatch)
        
        # Create Security configuration for the job
        self.glue_security_configuration = glue_alpha.SecurityConfiguration(
            self,
            f"{self.job_name}-security-configuration",
            security_configuration_name=f"{self.job_name}-security-configuration",
            s3_encryption={"mode": glue_alpha.S3EncryptionMode.S3_MANAGED},
            job_bookmarks_encryption=glue_alpha.JobBookmarksEncryption(
                mode=glue_alpha.JobBookmarksEncryptionMode.CLIENT_SIDE_KMS,
                kms_key=self.kms_job
            ),
            cloud_watch_encryption=glue_alpha.CloudWatchEncryption(
                mode=glue_alpha.CloudWatchEncryptionMode.KMS,
                kms_key=self.kms_job
            )       
        )

        # big query ga4 analytics table prefix
        self.table = self.dataset_id + ".events_intraday"
        
        arguments = {
            "--class":	"GlueApp",
            "--job-language":	"python",
            "--job-bookmark-option":	"job-bookmark-enable",
            "--TempDir":	f"s3://{self.glue_asset_bucket.bucket_name}/temporary/",
            "--enable-metrics":	"true",
            "--enable-continuous-cloudwatch-log":	"true",
            "--enable-spark-ui":	"true",
            "--enable-auto-scaling":	"true",
            "--spark-event-logs-path":	f"s3://{self.glue_asset_bucket.bucket_name}/sparkHistoryLogs/",
            "--enable-glue-datacatalog":	"true",
            "--enable-job-insights":	"true",
            "--table":	self.table,
            "--timedelta_days": self.timedelta_days,
            "--parentProject": self.parent_project,
            "--connectionName": self.connection_name,
            "--filter": self.filter,
            "--databucket": self.data_bucket.bucket_name,
            "--gluedatabasename": self.glue_database_name,
            "--gluetablename": self.glue_table_name
        }

        self.glue_job = glue_alpha.Job(self, f"{self.job_name}-id",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V3_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(self.data_bucket, "glue-scripts/" + self.job_script)
            ),
            job_name=self.job_name,
            role=self.role,
            default_arguments=arguments,
            security_configuration=self.glue_security_configuration,
            connections=[glue_alpha.Connection.from_connection_name(self, "bigquery-job-connection", self.connection_name)],
            description="BigQuery Connector Glue GA4 Data pull and transform"
        )

    ##############################################################################
    # GlueJob script
    ##############################################################################

    def add_scripts(self):
        s3_deploy.BucketDeployment(
            self,
            "script-deployment",
            sources=[s3_deploy.Source.asset("./assets/scripts")],
            destination_bucket=self.data_bucket,
            destination_key_prefix="glue-scripts",
        )

        
    ##############################################################################
    # GlueJob roles
    ##############################################################################

    def add_role(self):
        self.role = iam.Role(
            self,
            f"bigquery-glue-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        self.role.attach_inline_policy(
            iam.Policy(
                self,
                "glue_role_policy",
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:AssociateKmsKey"
                        ],
                        resources=[
                            f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/jobs/logs-v2:*"
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:GetBucketAcl",
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                        ],
                        resources=[f"{self.data_bucket.bucket_arn}/*"],
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "secretsmanager:GetResourcePolicy",
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:DescribeSecret",
                            "secretsmanager:ListSecretVersionIds",
                        ],
                        resources=[
                            self.gcp_secret.secret_arn
                        ],
                    ),
                ],
            )
        )

        # use managed roles for glue
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly"))
        self.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchLogsFullAccess'))
        self.gcp_secret.grant_read(self.role)
        
    ##############################################################################
    # GlueCrawler
    ##############################################################################

    def add_crawler(self):

        # Create Glue crawler's IAM role
        self.glue_crawler_role = iam.Role(
            self, 'GlueCrawlerRole',
            assumed_by=iam.ServicePrincipal(
                'glue.amazonaws.com'),
        )
        self.glue_crawler_role.attach_inline_policy(
            iam.Policy(
                self,
                "glue_crawler_role_policy",
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:GetBucketAcl",
                            "s3:GetObject",
                        ],
                        resources=[f"{self.data_bucket.bucket_arn}/*"]
                    )
                ]
            )
        )

        # Add managed policies to Glue crawler role
        self.glue_crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))


        # create Database
        glue_database= glue_alpha.Database(
            self,
            id=self.glue_database_name,
            database_name=self.glue_database_name
        )
        # Delete the database when deleting
        glue_database.apply_removal_policy(policy=RemovalPolicy.DESTROY)
        
        self.audit_policy = glue.CfnCrawler.SchemaChangePolicyProperty(update_behavior='UPDATE_IN_DATABASE', delete_behavior='LOG')
        
        self.glue_crawler = glue.CfnCrawler(self,f"{self.job_name}-crawler",
            name= f"{self.job_name}-crawler",
            role=self.glue_crawler_role.role_arn,
            database_name=self.glue_database_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets= [glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{self.data_bucket.bucket_name}/{self.glue_table_name}/",
                    exclusions= ["glue-scripts/**"],
                    sample_size=100
                )]
            ),
            schema_change_policy=self.audit_policy,
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )

    ##############################################################################
    # Step Function to managed the job flow
    ##############################################################################

    def add_workflow(self):

        succeed_nothing_to_job = sfn.Succeed(
            self, "Success",
            comment='Job succeeded'
        )

        # Glue task for bigquery data fetch
        glue_job_task= sfn_tasks.GlueStartJobRun(self, "StartBigQueryGlueJob",
            glue_job_name= self.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(30),
            notify_delay_after=Duration.minutes(5)
        )

        start_crawler_task = sfn_tasks.CallAwsService(self,
            "StartGlueCrawlerTask",
            service="glue",
            action="startCrawler",
            parameters={
                "Name": self.glue_crawler.name
            },
            iam_resources=[
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:*"
            ]
        )
        
        get_crawler_status_task = sfn_tasks.CallAwsService(self,
            "GetCrawlerStatusTask",
            service="glue",
            action="getCrawler",
            parameters={
                "Name": self.glue_crawler.name
            },
            iam_resources=[
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:*"
            ]
        )

        catch_job_error = sfn.Pass(
            self,
            "Catch an Error",
            result_path=JsonPath.DISCARD
        )

        job_failed = sfn.Fail(self, "Job Failed",
            cause="Job Failed",
            error="JOB FAILED"
        )

        # catch error and retry
        glue_job_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        glue_job_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["GlueJobRetry"])
        start_crawler_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        start_crawler_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["CrawlerRetry"])
        
        catch_job_error.next(job_failed)

        # check the crawler status
        wait_90m=sfn.Wait(self,"Wait 90 Seconds",
            time=sfn.WaitTime.duration(Duration.seconds(90)))

        wait_90m.next(get_crawler_status_task).next(sfn.Choice(self, 'Crawler Finished?')\
            .when(sfn.Condition.string_equals("$.Crawler.State", "READY"), succeed_nothing_to_job)
            .otherwise(wait_90m))

        definition = glue_job_task.next(start_crawler_task).next(wait_90m)\

        # Create state machine
        sm = sfn.StateMachine(
            self, "gcp-connector-glue",
            state_machine_name =f'gcp-connector-glue',
            definition=definition,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "UnfurlStateMachineLogGroup"),
                level=sfn.LogLevel.ALL,
            ),
            timeout=Duration.minutes(1800),
            tracing_enabled=True,
        )

    ##############################################################################
    # EventBridge Trigger Cron
    ##############################################################################

        scheduling_rule = events.Rule(
            self, "Scheduling Rule",
            rule_name=f'BigQuery-Job-Scheduling-Rule',
            description="Daily triggered event to get analytics data",
            schedule=events.Schedule.cron(
                minute='0',
                hour=self.schedule,
                ),
        )
        
        scheduling_rule.add_target(targets.SfnStateMachine(sm))        
        