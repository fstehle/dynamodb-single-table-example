package common

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

type TableManager struct {
	dynamoDBClient dynamodbiface.DynamoDBAPI
	tableName      string
}

func NewTableManager(dynamoDBClient dynamodbiface.DynamoDBAPI, tableName string) *TableManager {
	return &TableManager{
		dynamoDBClient: dynamoDBClient,
		tableName:      tableName,
	}
}

func (r *TableManager) CreateTable() error {
	log.WithField("table", r.tableName).Info("Creating the dynamoDB table")

	_, err := r.dynamoDBClient.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(r.tableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("data"),
				AttributeType: aws.String("S"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi_1"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("sk"),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
					{
						AttributeName: aws.String("data"),
						KeyType:       aws.String(dynamodb.KeyTypeRange),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				},
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
	})
	if err != nil {
		return fmt.Errorf("could not create dynamoDB table %v: %v", r.tableName, err)
	}

	err = r.dynamoDBClient.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(r.tableName),
	})
	if err != nil {
		return fmt.Errorf("error waiting for dynamoDB table %v creation: %v", r.tableName, err)
	}

	log.WithField("table", r.tableName).Info("Created table")

	return nil
}

func (r *TableManager) DeleteTable() error {
	log.WithField("table", r.tableName).Info("Deleting the dynamoDB table")

	_, err := r.dynamoDBClient.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(r.tableName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			// Table already deleted -> do nothing
			log.WithField("table", r.tableName).Info("Table already deleted")
			return nil
		}
		return fmt.Errorf("could not delete dynamoDB table %v: %v", r.tableName, err)
	}

	err = r.dynamoDBClient.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(r.tableName),
	})
	if err != nil {
		return fmt.Errorf("error waiting for dynamoDB table %v deletion: %v", r.tableName, err)
	}

	log.WithField("table", r.tableName).Info("Deleted table")

	return nil
}

func (r *TableManager) PurgeTable() error {
	log.WithField("table", r.tableName).Info("Purging the dynamoDB table")

	err := r.dynamoDBClient.ScanPages(&dynamodb.ScanInput{
		TableName: aws.String(r.tableName),
		ProjectionExpression:aws.String("pk, sk"),
		Limit: aws.Int64(25),

	}, func(output *dynamodb.ScanOutput, b bool) bool {
		if len(output.Items) == 0 {
			return true
		}

		var writeRequests []*dynamodb.WriteRequest
		for _, item := range output.Items {
			writeRequests = append(writeRequests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"sk": {
							S: item["sk"].S,
						},
						"pk": {
							S: item["pk"].S,
						},
					},
				},
			})
		}

		_, err := r.dynamoDBClient.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				r.tableName: writeRequests,
			},
		})
		if err != nil {
			log.WithError(err).Errorf("Error deleting items in batchWriteItem request")
		}

		return true
	})
	if err != nil {
		return fmt.Errorf("error scanning dynamoDB table %v: %v", r.tableName, err)
	}

	log.WithField("table", r.tableName).Info("Purged table")

	return nil
}
