package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "os"
)

type S3Manager struct {
	svc			*s3.S3
	uploader	*s3manager.Uploader
	downloader	*s3manager.Downloader
}

func (s *S3Manager) Init(accessKeyID, secretAccessKey, region string) error {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		return err
	}

	// check if credentials have been found
	_, err = sess.Config.Credentials.Get()
	if err != nil {
		return err
	}

	s.uploader = s3manager.NewUploader(sess)
	s.downloader = s3manager.NewDownloader(sess)
	s.svc = s3.New(sess)
	return nil	
}

func (s *S3Manager) InitAuto() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// check if creadentials have been found
	_, err := sess.Config.Credentials.Get()
	if err != nil {
		return err
	}

	s.uploader = s3manager.NewUploader(sess)
	s.downloader = s3manager.NewDownloader(sess)
	s.svc = s3.New(sess)
	return nil
}

func (s *S3Manager) CreateBucket(bucketname string) error {
	_, err := s.svc.CreateBucket(&s3.CreateBucketInput{
		Bucket : aws.String(bucketname),
	})

	if err != nil {
		return err
	}

	// Wait until bucket is created before finishing
	err = s.svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket : aws.String(bucketname),
	})

	return err
}

func (s *S3Manager) ListBuckets() (bucketnames []string, err error) {
	result, err := s.svc.ListBuckets(nil)
	if err != nil {
		return nil, err
	}
	for _, bucket := range result.Buckets {
		bucketnames = append(bucketnames, aws.StringValue(bucket.Name))
	}
	return
}

func (s *S3Manager) ListBucketsItemNames(bucketname string) (itemnames []string, err error) {
	resp, err := s.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket : aws.String(bucketname),
	})

	if err != nil {
		return nil, err
	}

	for _, item := range resp.Contents {
		itemnames = append(itemnames, *item.Key)
	}
	return
}

func (s *S3Manager) CreateBucketIfNotExists(bucketname string) error {
	bucketnames, err := s.ListBuckets()
	if err != nil {
		return err
	}

	for _, b := range bucketnames {
		if b == bucketname {
			return nil
		}
	}

	return s.CreateBucket(bucketname)
}

func (s *S3Manager) UploadFile(bucketname string, file *os.File) error {
	filename := file.Name()
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket : aws.String(bucketname),
		Key : aws.String(filename),
		Body : file,
	})

	// wait until the object is added
	err = s.svc.WaitUntilObjectExists(&s3.HeadObjectInput{
	    Bucket: aws.String(bucketname),
	    Key:    aws.String(filename),
	})

	return err
}

func (s *S3Manager) DeleteItem(bucketname, itemKey string) error {
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket : aws.String(bucketname),
		Key : aws.String(itemKey),
	})

	if err != nil {
		return err
	}

	// wait until the object is deleted
	err = s.svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
	    Bucket: aws.String(bucketname),
	    Key:    aws.String(itemKey),
	})

	return err
}

func (s *S3Manager) DeleteBucket(bucketname string) error {
	_, err := s.svc.DeleteBucket(&s3.DeleteBucketInput{
	    Bucket: aws.String(bucketname),
	})
	if err != nil {
		return err
	}

	err = s.svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
	    Bucket: aws.String(bucketname),
	})

	return err
}

func (s *S3Manager) DownloadItem(bucketname, itemKey string) (file *os.File, err error) {
	_, err = s.downloader.Download(file, &s3.GetObjectInput{
        Bucket: aws.String(bucketname),
        Key:    aws.String(itemKey),
    })
    return
}