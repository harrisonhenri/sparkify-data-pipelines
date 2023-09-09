from boto3 import client, resource
from mypy_boto3_iam import IAMServiceResource
from mypy_boto3_s3 import S3ServiceResource

from src.config import AWS, S3, IAM
from operator import itemgetter

REGION = AWS.get("REGION")

# S3
SOURCE_BUCKET = S3.get("SOURCE_BUCKET")
BUCKET_NAME = S3.get("BUCKET_NAME")

# IAM user
USER = IAM.get("SPARKIFY_USER")
IAM_USER_POLICY_TEMPLATE = "arn:aws:iam::aws:policy"
ADMIN_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AdministratorAccess"
S3_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AmazonS3FullAccess"
REDSHIFT_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AmazonRedshiftFullAccess"

# Redshift role
ROLE = IAM.get("SPARKIFY_ROLE")
REDSHIFT_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}"""


def clients():
    s3 = resource("s3", region_name="us-east-1")
    ec2 = resource("ec2", region_name=REGION)
    iam = resource("iam", region_name=REGION)
    redshift = client("redshift-serverless", region_name=REGION)

    return {"s3": s3, "ec2": ec2, "iam": iam, "redshift": redshift}


def s3_provisioning():
    s3: S3ServiceResource = itemgetter("s3")(clients())

    s3.create_bucket(Bucket=BUCKET_NAME)

    source_bucket = s3.Bucket(SOURCE_BUCKET).objects

    for file in source_bucket.filter(Prefix="log_data"):
        s3.meta.client.copy(
            {"Bucket": SOURCE_BUCKET, "Key": file.key}, BUCKET_NAME, file.key
        )
    for file in source_bucket.filter(Prefix=f"song_data/A/{letter}"):
        s3.meta.client.copy(
            {"Bucket": SOURCE_BUCKET, "Key": file.key}, BUCKET_NAME, file.key
        )


def iam_provisioning():
    iam: IAMServiceResource = itemgetter("iam")(clients())

    # IAM user
    iam.create_user(UserName=USER)
    iam.meta.client.attach_user_policy(UserName=USER, PolicyArn=ADMIN_USER_POLICY)
    iam.meta.client.attach_user_policy(UserName=USER, PolicyArn=REDSHIFT_USER_POLICY)
    iam.meta.client.attach_user_policy(UserName=USER, PolicyArn=S3_USER_POLICY)

    # Redshift
    iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=REDSHIFT_POLICY)
    iam.meta.client.attach_role_policy(RoleName=ROLE, PolicyArn=S3_USER_POLICY)


if __name__ == "__main__":
    s3_provisioning()
    iam_provisioning()
