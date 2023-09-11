import boto3
import io
import zipfile
import os
s3 = boto3.client('s3')

def uploadToS3(fileToUpload, bucketName, s3Path):
    try:
        # Upload a file to the specified bucket
        s3.upload_file('./zips/' + fileToUpload,bucketName,s3Path)
        # s3.put_object(Bucket=bucketName, Key=s3Path, Body=fileToUpload)
        print("Upload successful!")
    except FileNotFoundError:
        print(f"The file '{fileToUpload}' was not found.")
    except NoCredentialsError:
        print("Credentials not available. Make sure you have AWS credentials configured.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return True

def iterate_bucket_items(bucketName):
    """
    Generator that iterates over all objects in a given s3 bucket

    See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2 
    for return data format
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """


    
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucketName)
    localPath = "./zips/"

    for page in page_iterator:
        if page['KeyCount'] > 0:
            for item in page['Contents']:
                fileKey = item['Key']
                if(fileKey.endswith('.zip')):
                    sendTo = fileKey.replace("unprocessed", "processed").replace('.zip','')
                    print(f"Unzipping and deleting {fileKey}")
                    zip_obj = s3.get_object(Bucket=bucketName, Key=fileKey)
                    with zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read())) as zip_ref:
                        zip_ref.extractall(localPath)
                    files = [f for f in os.listdir(localPath) if os.path.isfile(os.path.join(localPath, f))]
                    for file in files:
                        if(uploadToS3(file,bucketName,sendTo)):
                            s3.delete_object(Bucket=bucketName, Key=fileKey)
                        try:
                            os.remove('./zips/' + file)
                            print(f"Deleted file: {file}")
                        except OSError as e:
                            print(f"Error deleting file: {file}, {e}")

                        




                    # uploadToS3(zip_data, 'eg-datapipeline-valorant-dev', sendTo)
                    

iterate_bucket_items('eg-datapipeline-valorant-dev')
print("DONE")