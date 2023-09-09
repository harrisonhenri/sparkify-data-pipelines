from boto3 import client, resource
from mypy_boto3_iam import IAMServiceResource
from mypy_boto3_s3 import S3ServiceResource

from src.config import AWS, LAKEHOUSE, IAM
from operator import itemgetter

REGION = AWS.get("REGION")
USER = IAM.get("USER")
ROLE = IAM.get("ROLE")
BUCKET_NAME = LAKEHOUSE.get("BUCKET_NAME")
SERVICE_NAME = f"com.amazonaws.{REGION}.s3"

# IAM user
IAM_USER_POLICY_TEMPLATE = "arn:aws:iam::aws:policy"
ADMIN_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AdministratorAccess"
S3_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AmazonS3FullAccess"
REDSHIFT_USER_POLICY = f"{IAM_USER_POLICY_TEMPLATE}/AmazonRedshiftFullAccess"

# Redshift user
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
    s3 = resource("s3", region_name=REGION)
    ec2 = resource("ec2", region_name=REGION)
    iam = resource("iam", region_name=REGION)
    redshift = client("redshift-serverless", region_name=REGION)

    return {"s3": s3, "ec2": ec2, "iam": iam, "redshift": redshift}


def s3_decommissioning():
    s3: S3ServiceResource = itemgetter("s3")(clients())

    bucket = s3.Bucket(BUCKET_NAME)
    bucket.objects.all().delete()
    bucket.delete()


def iam_decommissioning():
    iam: IAMServiceResource = itemgetter("iam")(clients())

    # IAM user
    iam.meta.client.delete_user_policy(UserName=USER, PolicyName="AdministratorAccess")
    iam.meta.client.delete_user_policy(
        UserName=USER, PolicyName="AmazonRedshiftFullAccess"
    )
    iam.meta.client.delete_user_policy(UserName=USER, PolicyName="AmazonS3FullAccess")
    iam.meta.client.delete_user(UserName=USER)

    # Redshift
    iam.meta.client.delete_role_policy(RoleName=ROLE, PolicyArn="AmazonS3FullAccess")
    iam.meta.client.delete_role(RoleName=ROLE)


if __name__ == "__main__":
    s3_decommissioning()
    iam_decommissioning()
