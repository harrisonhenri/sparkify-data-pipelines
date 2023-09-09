import os

from decouple import config

os.environ["AWS_PROFILE"] = config("AWS_PROFILE")


AWS = {
    "REGION": config("REGION"),
}

IAM = {
    "SPARKIFY_USER": config("SPARKIFY_USER"),
    "SPARKIFY_ROLE": config("SPARKIFY_ROLE"),
}

S3 = {
    "SOURCE_BUCKET": config("SOURCE_BUCKET"),
    "BUCKET_NAME": config("BUCKET_NAME"),
}
