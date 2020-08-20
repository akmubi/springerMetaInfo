package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"fmt"
	"errors"
)

// DynamoDB wrapper
type DataBase struct {
	svc	*dynamodb.DynamoDB
}

func (db *DataBase) Init(accessKeyID, secretAccessKey, region string) error {
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

	db.svc = dynamodb.New(sess)
	return nil
}

func (db *DataBase) InitAuto() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// check if creadentials have been found
	_, err := sess.Config.Credentials.Get()
	if err != nil {
		return err
	}

	db.svc = dynamodb.New(sess)
	return nil
}

func (db *DataBase) ListTables() (tableNames []string, err error) {
	input := &dynamodb.ListTablesInput{}

	for {
		// Get the list of tables
		result, err := db.svc.ListTables(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeInternalServerError:
					return nil, errors.New(fmt.Sprint(dynamodb.ErrCodeInternalServerError, aerr.Error()))
				default:
					return nil, errors.New(aerr.Error())
				}
			} else {
				return nil, err
			}
		}

		for _, n := range result.TableNames {
			tableNames = append(tableNames, *n)
		}

		input.ExclusiveStartTableName = result.LastEvaluatedTableName
		if result.LastEvaluatedTableName == nil {
			break
		}
	}
	return
}

func (db *DataBase) waitUntilTableBecomeActive(tablename string) error {
	input := &dynamodb.DescribeTableInput{
		TableName : aws.String(tablename),
	}

	for {
		output, err := db.svc.DescribeTable(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeResourceNotFoundException:
					return errors.New(fmt.Sprint(dynamodb.ErrCodeResourceNotFoundException, aerr.Error()))
				case dynamodb.ErrCodeInternalServerError:
					return errors.New(fmt.Sprint(dynamodb.ErrCodeInternalServerError, aerr.Error()))
				default:
					return errors.New(aerr.Error())
				}
			} else {
				return err
			}
		}

		if *output.Table.TableStatus == "ACTIVE" {
			break
		} 
	}
	return nil
}

func (db *DataBase) CreateTableIfNotExists(tablename, primaryKey, primaryKeyType, sortKey, sortKeyType string) error {
	tables, err := db.ListTables()
	if err != nil {
		return err
	}

	// find tablename match
	for _, t := range tables {

		// if matches just return, do nothing
		if t == tablename {
			return nil
		}
	}

	// else create one
	if sortKey == "" && sortKeyType == "" {
		err = db.CreateTable(tablename, primaryKey, primaryKeyType)
	} else {
		err = db.CreateTableWithSort(tablename, primaryKey, primaryKeyType, sortKey, sortKeyType)
	}

	return err
}

func (db *DataBase) CreateTableWithSort(tablename, primaryKey, primaryAttributeType, sortKey, sortKeyType string) error {
	if primaryAttributeType != "N" && primaryAttributeType != "S" {
		return errors.New("Incorrect primary key type. Should be 'N' (Number) or 'S' (String)")
	}
	if sortKeyType != "N" && sortKeyType != "S" {
		return errors.New("Incorrect sort key tpe. Should be 'N' (Number) or 'S' (String)")
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(primaryKey),
				AttributeType: aws.String(primaryAttributeType),
			},
			{
				AttributeName: aws.String(sortKey),
				AttributeType: aws.String(sortKeyType),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(primaryKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName:	aws.String(sortKey),
				KeyType:		aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tablename),
	}

	if _, err := db.svc.CreateTable(input); err != nil {
		return err
	}

	return db.waitUntilTableBecomeActive(tablename)
}

func (db *DataBase) CreateTable(tablename, primaryKey, primaryAttributeType string) error {
	if primaryAttributeType != "N" && primaryAttributeType != "S" {
		return errors.New("Incorrect primary key type. Should be 'N' (Number) or 'S' (String)")
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(primaryKey),
				AttributeType: aws.String(primaryAttributeType),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(primaryKey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tablename),
	}

	if _, err := db.svc.CreateTable(input); err != nil {
		return err
	}

	return db.waitUntilTableBecomeActive(tablename)
}

func (db *DataBase) DeleteTable(tablename string) error {
	input := &dynamodb.DeleteTableInput {
		TableName: aws.String(tablename),
	}
	
	if _, err := db.svc.DeleteTable(input); err != nil {
		return err
	}
	return nil
}

func (db *DataBase) DeleteItem(tablename, primaryKeyName, primaryAttributeType, primaryKeyValue string) error {
	if primaryAttributeType != "N" && primaryAttributeType != "S" {
		return errors.New("Incorrect primary key type. Should be 'N' (Number) or 'S' (String)")
	}

	var attributeValue map[string]*dynamodb.AttributeValue
	if primaryAttributeType == "N" {
		attributeValue = map[string]*dynamodb.AttributeValue{ primaryKeyName : { N: aws.String(primaryKeyValue) } }
	} else {
		attributeValue = map[string]*dynamodb.AttributeValue{ primaryKeyName : { S: aws.String(primaryKeyValue) } }
	}

	input := &dynamodb.DeleteItemInput{
		Key: attributeValue,
		TableName: aws.String(tablename),
	}

	_, err := db.svc.DeleteItem(input)

	return err
}

func (db *DataBase) PutItem(tablename string, item interface{}) error {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput {
		Item : av,
		TableName : aws.String(tablename),
	}

	_, err = db.svc.PutItem(input)
	return err
}