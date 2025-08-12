import os
import subprocess

LAST_GOOD_RELEASE = "s3://my-bucket/last_good_release.tar.gz"

def rollback():
    print("Deployment failed. Rolling back...")
    subprocess.run(["aws", "s3", "cp", LAST_GOOD_RELEASE, "."])
    subprocess.run(["tar", "-xzf", "last_good_release.tar.gz"])
    subprocess.run(["bash", "deploy.sh", "stable"])

if __name__ == "__main__":
    rollback()
