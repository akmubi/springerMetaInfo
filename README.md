# springerMetaParserUltraDoNotUse

## Install
Download latest executable in [*releases*](https://github.com/akmubi/springerMetaParserUltraDoNotUse/releases)

## Prerequisites
1. Before using executable you must get **an access key ID** and **a secret access key** from your administrator in Amazon IMA and ask for following rights:
+ ListTables
+ DescribeTable
+ CreateTable
+ DeleteItem
+ DeleteTable
+ PutItem
2. If you want to upload PDF files to S3, ask your administrator for theese rights:
+ ListBucket
+ CreateBucket
+ ListAllMyBuckets
+ DeleteBucket
+ PutObject
+ GetObject
+ DeleteObject
3. Find out database region
4. Save your **access key ID** and **secret access key** in ***%USERPFOLILE%/.aws/credentials*** (***~/.aws/credentials***) file in following format:
```
[default]
aws_access_key_id=YOUR ACCESS KEY ID
aws_secret_access_key=YOUR SECRET ACCESS KEY
```
OR
If you don't want to create a ***credentials*** file , you can specify them by following command-line flags:
```shell
>springerMetaInfo.exe ... -accesskey="YOUR ACCESS KEY ID" -secretkey="YOUR SECRET ACCESS KEY" ...
```
5. Save your **region** in ***%USERPROFILE%/.aws/config*** (***~/.aws/config***) file in following format:
```
[default]
   region=YOUR REGION
   output=json
```
OR
if you don't want to create a ***config*** file, you can specify *region* by:
+ environment variable
```shell
>set AWS_REGION="YOUR REGION"
```
+ command-line flag
```shell
>springerMetaInfo.exe ... -region="YOUR REGION" ...
```

## Basic Usage
### Command
```shell
>springerMetaInfo.exe -accesskey="..." -secretkey="..." -region="us-east-2" \
-keywords="decompilation" -tablename="SampleTable" \
-pkname="Title" -pktype=S -openaccess -maxpages=3
```
### Output:
```
Table info:
        Name: SampleTable
        Primary Key: Title
        Primary Key Type: S
Connecting to database...
Checking table - SampleTable
Found 14 records
Number of pages to parse: 2
Passed: 1 page(-s)
Passed: 2 page(-s)

No parser errors encountered

Items inserted into database - 14

Success! Elapsed - 15.0721212s
```
## Other options
Type --help to see other options
```shell
>springerMetaInfo.exe --help
Usage of springerMetaInfo.exe:
  -accesskey string
        Amazom DynamoDB Access Key ID
  -bucketname string
        S3 bucket name to upload into. Example -bucketname="myuniquebucketname3287"
  -keywords string
        keywords to search in Springer. Example: -keywords="decompilation techniques"
  -maxpages int
        Max number of pages to parse. If you want to parse all pages use -1. Example: -maxpages=200 (default 100)
  -openaccess
        Parse only Open Access articles. Example: -openaccess
  -pkname string
        Primary Key name. Example: -pkname="Publisher"
  -pktype string
        Primary Key type. Possible types - "N"/"S" (Number/String). Example: -pktype=N
  -records int
        Number of records (meta info) in page (max - 50). Example: -records=35 (default 10)
  -region string
        Amazom DynamoDB Region
  -routines int
        Number of routines. Example: -routines=30 (default 10)
  -secretkey string
        Amazom DynamoDB Secret Access Key ID
  -skname string
        Sort Key name. Example: -skname="ID"
  -sktype string
        Sort Key type. Possible types - "N"/"S" (Number/String). Example: -sktype=N
  -tablename string
        Table name to upload into. Example: -tablename="Music"
  -timeout int
        Timeout duration in seconds (for routines). Should be at least 1 second. Example: -timeout=5 (default 1)
```
