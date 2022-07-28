import  boto3
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
# Create spark session
spark = SparkSession.builder.appName('src_s3_to_target_s3').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")  # to avoid temp folder creation
hadoopConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
hadoopConf.set("spark.hadoop.avro.mapred.ignore.inputs.without.extension", "false")
hadoopConf.set("avro.mapred.ignore.inputs.without.extension", "false")
src_bucket='<bucketname>'
src_bucket_prefix='iss/'
tgt_bucket='<bucketname>'
tgt_bucket_prefix = 'iss/'
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket = s3_resource.Bucket(src_bucket)
#Method to check if folder exists
def get_list_of_file(path_prefix):
    try:
        print("** get_list_of_file(): started **")
        file_list = []
        for each_obj in bucket.objects.filter(Prefix=path_prefix):
            if '.csv' in each_obj.key:
                file_path = each_obj.key
                print("File Path: " + str(file_path))
                file_name = file_path.split("/")[-1]
                print("File Name: " + str(file_name))
                last_modified_time = each_obj.last_modified
                print("Last Modified Date:", last_modified_time)
                file_list.append(file_name)
        print("** get_list_of_file(): completed **")
        return file_list
    except Exception as e:
        print(e)
def file_exist(path_prefix):
    print("** file_exist(): started **")
    lst = get_list_of_file(path_prefix)
    print(lst)
    now = datetime.now(timezone.utc)
    current_date = now.strftime("%Y%m%d")
    print("current_date:", current_date)
    file_list = ['person', 'acom_holdout']
    for file in file_list:
        file_name = f'{file}_{current_date}.csv'
        if file_name in lst:
            print(f'Yes exist... {file_name}')
            read_src_s3(file_name, file)
        else:
            print('Not exist...')
    print("** file_exist(): completed **")
def read_src_s3(file_name, file):
    try:
        print("*****Inside fetchValidEvents() function*****")
        srcFullPath=f's3a://{src_bucket}/{src_bucket_prefix}{file_name}'
        file_name = srcFullPath.split("/")[-1]
        print("FileName: " + str(file_name))
        getTimestamp = file_name.split(".")[0][-8:]
        print("File Timestamp: " + str(getTimestamp))
        year = getTimestamp[:4]
        print(year)
        month = getTimestamp[4:6:1]
        print(month)
        day = getTimestamp[6:8:1]
        print(day)
        print("Source S3 Path : " + str(srcFullPath))
        df = spark.read.format("csv") \
            .option("delimiter", ",") \
            .option("inferSchema", "false") \
            .option("quote", "") \
            .load(srcFullPath)
        df.write.format("parquet").mode("overwrite").save(
            f"s3a://{tgt_bucket}/{tgt_bucket_prefix}/{file}/year={year}/month={month}/day={day}")
    except Exception as e:
        print(e)
if _name_ == "_main_":
    print("** Inside @main method **")
    file_exist(f'{src_bucket_prefix}')