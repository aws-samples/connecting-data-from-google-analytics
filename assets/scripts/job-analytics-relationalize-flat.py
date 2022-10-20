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
from pyspark.sql.functions import col, lit, explode, collect_list, struct

# Glue script to handle the complex data types such as event_params

sc = SparkContext.getOrCreate()
args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket','gluetablename','timedelta_days'])
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()


gluetablename=str(args['gluetablename'])
table=str(args['table'])
databucket= str(args['databucket'])
timedelta_days= int(args['timedelta_days'])
now = datetime.today() - timedelta(days=timedelta_days)
format = "%Y%m%d"
table_suffix = now.strftime(format)
table_full = table +"_"+ table_suffix
tmp_staging_path = f"s3://{databucket}/temp"


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def leftJoinTable(glueContext, datasource, right_df) -> DynamicFrame:
    datasource0_DF = datasource.toDF()
    right_df = right_df.toDF()
    result = DynamicFrame.fromDF(
        datasource0_DF.join(
            right_df,
            (
                datasource0_DF["join_key"]
                == right_df["`join_key`"]
            ),
            "leftouter",
        ),
        glueContext,
        "Join_node1663684948739",
    )
    return result

def flattenNestedData(glueContext, datasource, column_name,data_type) -> DynamicFrame:
    
    # get the join_key and the nest events column as an array
    SqlQuery0 = """
    SELECT join_key,array(event_params) as event_params from myDataSource

    """
    data_filtered= sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={"myDataSource": datasource},
        transformation_ctx="SQL_node1663596786370",
    )
    
    # pivot the events params and the join_key columns
    dfc = Relationalize.apply(frame = data_filtered, staging_path = tmp_staging_path, name = "root", transformation_ctx = "dfc")
    flattened_event_params_data = dfc.select("root_event_params.val")
    join_key_data = dfc.select("root")

    # join the event_params with the join_key
    join0 = Join.apply(
    frame1=flattened_event_params_data,
    frame2=join_key_data,
    keys1=["id"],
    keys2=["event_params"],
    transformation_ctx="join0",
    )

    # filter the event_param column name such as page_title
    SqlQuery0 = f"""
    SELECT * from myDataSource 
    where `event_params.val.val.key`= "{column_name}"
    """
    flat_data_filtered= sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={"myDataSource": join0},
        transformation_ctx="SQL_node1663596786370",
    )
    
    # rename the column to the event_param name
    result = RenameField.apply(
        frame = flat_data_filtered, 
        old_name = f"`event_params.val.val.value.{data_type}_value`",
        new_name = column_name
    )

    #  Drop unneeded Fields
    result = DropFields.apply(
        frame=result,
        paths=[
            "`event_params.val.val.key`",
            "`event_params.val.val.value.string_value`",
            "`event_params.val.val.value.int_value`",
            "event_params.val.val.key",
            "event_params",
            "id",
            "index"
        ],
        transformation_ctx="DropFields_node1663633725943",
    )
    
    return result

glueContext = GlueContext(spark.sparkContext)


# use the Marketplace glue connector for BigQuery
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
            "viewsEnabled": "true",
            "table": table_full.strip(),
            "parentProject": args["parentProject"],
            "connectionName": args["connectionName"]
    },
    transformation_ctx="datasource0 ",
)

# create a join key
SqlQuery0 = """
select 
concat(event_name,event_timestamp,user_pseudo_id) as join_key,*
from myDataSource
"""
datasource0_selectedfields = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": datasource0},
    transformation_ctx="SQL_node1662231599921",
)

# flatten event_params columns note: does not include all
page_title_flat= flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "page_title",
    data_type="string"
)
page_location_flat= flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "page_location",
    data_type="string"
)
ga_session_id_flat= flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "ga_session_id",
    data_type="int"
)
page_referrer_flat= flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "page_referrer",
    data_type="string"
)
percent_scrolled_flat= flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "percent_scrolled",
    data_type="string",
)
session_engaged_flat = flattenNestedData(
    glueContext,
    datasource=datasource0_selectedfields,
    column_name = "session_engaged",
    data_type="string",
)

# join the nested columns to the base table
join_all=leftJoinTable(glueContext, datasource0_selectedfields, page_title_flat)
join_all=leftJoinTable(glueContext, join_all, page_location_flat)
join_all=leftJoinTable(glueContext, join_all, ga_session_id_flat)
join_all=leftJoinTable(glueContext, join_all, page_referrer_flat)
join_all=leftJoinTable(glueContext, join_all, percent_scrolled_flat)
join_all=leftJoinTable(glueContext, join_all, session_engaged_flat)

#  Drop join_key field
join_all = DropFields.apply(
    frame=join_all,
    paths=[
        "join_key"
    ],
    transformation_ctx="DropFields_node1663633725943",
)

# reset the join_key
SqlQuery0 = """
select 
concat(event_name,event_timestamp,user_pseudo_id) as join_key,*
from myDataSource
"""
join_all = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": join_all},
    transformation_ctx="SQL_node1662231599921",
)

#  Drop unneeded fields
join_all = DropFields.apply(
    frame=join_all,
    paths=[
        ".percent_scrolled"
    ],
    transformation_ctx="DropFields_node1663633725943",
)

# sink the data to s3 partitioned by event_date
path = f"s3://{databucket}/{gluetablename}"

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=join_all,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": path,
        "partitionKeys": ["event_date"]},
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3"
)
                        
job.commit()