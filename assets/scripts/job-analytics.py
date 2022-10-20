import sys
import pytz
from datetime import timedelta
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


# simple glue script that does not handle the unnesting of the complex data types such as event_params
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket','gluetablename'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

gluetablename=str(args['gluetablename'])
table=str(args['table'])
databucket= str(args['databucket'])
now = datetime.today() - timedelta(days=1)
format = "%Y%m%d"

table_suffix = now.strftime(format)
table_full = table +"_"+ table_suffix


# Script generated for node analyticsdata
analyticsdata_node1659719961623 = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
            "viewsEnabled": "true",
            "table": table_full.strip(),
            "parentProject": args["parentProject"],
            "connectionName": args["connectionName"]
    },
    transformation_ctx="analyticsdata_node1659719961623",
)



# SQL 
SqlQuery0 = """
select 
concat(event_name,event_timestamp,user_pseudo_id) as join_key,
event_date as event_date,
event_name as event_name,
event_timestamp as event_timestamp,
event_previous_timestamp as event_previous_timestamp,
event_value_in_usd as event_value_in_usd,
event_bundle_sequence_id as event_bundle_sequence_id,
event_server_timestamp_offset as event_server_timestamp_offset,
user_id as user_id,
user_pseudo_id as user_pseudo_id,
stream_id as stream_id,
platform as platform,
event_params,			
privacy_info,			
user_properties,
geo,
user_ltv,						
app_info,			
traffic_source,			
event_dimensions,			
ecommerce,			
items

from myDataSource

"""
SQL_node1662231599921 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": analyticsdata_node1659719961623},
    transformation_ctx="SQL_node1662231599921",
)


# select fields
analyticsfields_node1659719907216 = SelectFields.apply(
    frame=SQL_node1662231599921,
    paths=[
        "join_key",
        "event_name",
        "event_params.key",
        "event_params.value.string_value",
        "event_params.value.int_value",
        "event_params.value.float_value",
        "event_params.value.double_value",
        "event_params.value",
        "event_params",
        "user_id",
        "event_date",
        "event_timestamp",
        "event_previous_timestamp",
        "event_value_in_usd",
        "event_bundle_sequence_id",
        "event_server_timestamp_offset",
        "user_pseudo_id",
        "stream_id",
        "platform",
        "privacy_info.analytics_storage",
        "privacy_info.ads_storage",
        "privacy_info.uses_transient_token",
        "privacy_info",
        "user_properties.key",
        "user_properties.value.string_value",
        "user_properties.value.int_value",
        "user_properties.value.float_value",
        "user_properties.value.double_value",
        "user_properties.value.set_timestamp_micros",
        "user_properties.value",
        "user_properties",
        "user_ltv.revenue",
        "user_ltv.currency",
        "user_ltv",
        "app_info.id",
        "app_info.version",
        "app_info.install_store",
        "app_info.firebase_app_id",
        "app_info.install_source",
        "app_info",
        "traffic_source.name",
        "traffic_source.medium",
        "traffic_source.source",
        "traffic_source",
        "event_dimensions.hostname",
        "event_dimensions",
        "ecommerce.total_item_quantity",
        "ecommerce.purchase_revenue_in_usd",
        "ecommerce.purchase_revenue",
        "ecommerce.refund_value_in_usd",
        "ecommerce.refund_value",
        "ecommerce.shipping_value_in_usd",
        "ecommerce.shipping_value",
        "ecommerce.tax_value_in_usd",
        "ecommerce.tax_value",
        "ecommerce.unique_items",
        "ecommerce.transaction_id",
        "ecommerce",
        "items.item_id",
        "items.item_name",
        "items.item_brand",
        "items.item_variant",
        "items.item_category",
        "items.item_category2",
        "items.item_category3",
        "items.item_category4",
        "items.item_category5",
        "items.price_in_usd",
        "items.price",
        "items.quantity",
        "items.item_revenue_in_usd",
        "items.item_revenue",
        "items.item_refund_in_usd",
        "items.item_refund",
        "items.coupon",
        "items.affiliation",
        "items.location_id",
        "items.item_list_id",
        "items.item_list_name",
        "items.item_list_index",
        "items.promotion_id",
        "items.promotion_name",
        "items.creative_name",
        "items.creative_slot",
        "items",
        "event_date_p",
        "geo.continent",
        "geo.country",
        "geo.region",
        "geo.city",
        "geo.sub_continent",
        "geo.metro",
        "geo",
    ],
    transformation_ctx="analyticsfields_node1659719907216",
)


path = f"s3://{databucket}/{gluetablename}"

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=analyticsfields_node1659719907216,
    connection_type="s3",
    format="json",
    connection_options={
        "path": path,
        "partitionKeys": ["event_date"],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()