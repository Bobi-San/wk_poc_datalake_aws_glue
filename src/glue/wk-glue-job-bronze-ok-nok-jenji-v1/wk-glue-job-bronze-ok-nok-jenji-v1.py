import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
import concurrent.futures
import re


class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters


def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)


def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print("%r generated an exception: %s" % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 DL Bronze Txn Jenji
S3DLBronzeTxnJenji_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="wk-glue-data-catalog-arrivalhub",
    table_name="tb_transaction_raw_pendval_jenji",
    transformation_ctx="S3DLBronzeTxnJenji_node1",
)

# Script generated for node Filter out columns
Filteroutcolumns_node2 = DropFields.apply(
    frame=S3DLBronzeTxnJenji_node1,
    paths=["_id.oid", "_id"],
    transformation_ctx="Filteroutcolumns_node2",
)

# Script generated for node Validate fields
Validatefields_node1684510082794 = threadedRoute(
    glueContext,
    source_DyF=Filteroutcolumns_node2,
    group_filters=[
        GroupFilter(
            name="good_records",
            filters=lambda row: (
                bool(re.match(".*", row["cardprivatepan"]))
                and bool(re.match(".*", row["cardprotransactionid"]))
                and bool(re.match(".*", row["category"]))
                and bool(
                    re.match(
                        "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                        row["createdat"],
                    )
                )
                and bool(re.match(".*", row["currency"]))
                and bool(re.match(".*", row["eventtype"]))
                and bool(re.match(".*", row["jenjiexpenseid"]))
                and bool(
                    re.match(
                        "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                        row["lastupdatedat"],
                    )
                )
                and bool(re.match(".*", row["seller"]))
                and bool(re.match(".*", row["state"]))
                and row["taxrecoverable"] >= 0
                and row["taxrecoverable"] <= 1
                and bool(
                    re.match(
                        "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                        row["time"],
                    )
                )
                and bool(
                    re.match("^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$", row["total"])
                )
                and bool(
                    re.match(
                        "^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$", row["totalwithouttax"]
                    )
                )
            ),
        ),
        GroupFilter(
            name="default_group",
            filters=lambda row: (
                not (
                    bool(re.match(".*", row["cardprivatepan"]))
                    and bool(re.match(".*", row["cardprotransactionid"]))
                    and bool(re.match(".*", row["category"]))
                    and bool(
                        re.match(
                            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                            row["createdat"],
                        )
                    )
                    and bool(re.match(".*", row["currency"]))
                    and bool(re.match(".*", row["eventtype"]))
                    and bool(re.match(".*", row["jenjiexpenseid"]))
                    and bool(
                        re.match(
                            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                            row["lastupdatedat"],
                        )
                    )
                    and bool(re.match(".*", row["seller"]))
                    and bool(re.match(".*", row["state"]))
                    and row["taxrecoverable"] >= 0
                    and row["taxrecoverable"] <= 1
                    and bool(
                        re.match(
                            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$",
                            row["time"],
                        )
                    )
                    and bool(
                        re.match("^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$", row["total"])
                    )
                    and bool(
                        re.match(
                            "^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$",
                            row["totalwithouttax"],
                        )
                    )
                )
            ),
        ),
    ],
)

# Script generated for node good_records
good_records_node1684510082995 = SelectFromCollection.apply(
    dfc=Validatefields_node1684510082794,
    key="good_records",
    transformation_ctx="good_records_node1684510082995",
)

# Script generated for node default_group
default_group_node1684510082986 = SelectFromCollection.apply(
    dfc=Validatefields_node1684510082794,
    key="default_group",
    transformation_ctx="default_group_node1684510082986",
)

# Script generated for node S3 DL Bronze Ok Txn Jenji
S3DLBronzeOkTxnJenji_node3 = glueContext.getSink(
    path="s3://rgi-sandbox-repo-dev/DataLakeV1/ArrivalHub/Validated/Jenji/v1/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3DLBronzeOkTxnJenji_node3",
)
S3DLBronzeOkTxnJenji_node3.setCatalogInfo(
    catalogDatabase="wk-glue-data-catalog-arrivalhub",
    catalogTableName="tb_transaction_raw_valid_jenji_v1",
)
S3DLBronzeOkTxnJenji_node3.setFormat("glueparquet")
S3DLBronzeOkTxnJenji_node3.writeFrame(good_records_node1684510082995)
# Script generated for node S3 DL Bronze Nok Txn Jenji
S3DLBronzeNokTxnJenji_node1684510783099 = glueContext.write_dynamic_frame.from_options(
    frame=default_group_node1684510082986,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://rgi-sandbox-repo-dev/DataLakeV1/ArrivalHub/Rejected/Jenji/",
        "partitionKeys": [],
    },
    transformation_ctx="S3DLBronzeNokTxnJenji_node1684510783099",
)

job.commit()
