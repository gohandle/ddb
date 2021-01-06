package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func writeSingle(ctx context.Context, ddb Dynamo, wi *dynamodb.TransactWriteItem) (r Result, err error) {
	var attr map[string]*dynamodb.AttributeValue

	switch {
	case wi.Put != nil:
		in := &dynamodb.PutItemInput{
			TableName:                 wi.Put.TableName,
			Item:                      wi.Put.Item,
			ConditionExpression:       wi.Put.ConditionExpression,
			ExpressionAttributeNames:  wi.Put.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Put.ExpressionAttributeValues,
		}

		var out *dynamodb.PutItemOutput
		if out, err = ddb.PutItemWithContext(ctx, in); err != nil {
			return nil, fmt.Errorf("failed to put item %v: %w", in, err)
		}

		attr = out.Attributes
	case wi.Delete != nil:
		var out *dynamodb.DeleteItemOutput
		if out, err = ddb.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
			TableName:                 wi.Delete.TableName,
			Key:                       wi.Delete.Key,
			ConditionExpression:       wi.Delete.ConditionExpression,
			ExpressionAttributeNames:  wi.Delete.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Delete.ExpressionAttributeValues,
		}); err != nil {
			return nil, fmt.Errorf("failed to delete item: %w", err)
		}

		attr = out.Attributes
	case wi.Update != nil:
		var out *dynamodb.UpdateItemOutput
		if out, err = ddb.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName:                 wi.Update.TableName,
			Key:                       wi.Update.Key,
			UpdateExpression:          wi.Update.UpdateExpression,
			ConditionExpression:       wi.Update.ConditionExpression,
			ExpressionAttributeNames:  wi.Update.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Update.ExpressionAttributeValues,
		}); err != nil {
			return nil, fmt.Errorf("failed to update item: %w", err)
		}

		attr = out.Attributes
	default:
		return nil, fmt.Errorf("unsupported single operation: %v", wi)
	}

	if attr == nil {
		return emptyResult{}, nil
	}

	return newResult(attr), nil
}

func readSingle(ctx context.Context, ddb Dynamo, ri *dynamodb.TransactGetItem) (r Result, err error) {
	var out *dynamodb.GetItemOutput
	if out, err = ddb.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:                ri.Get.TableName,
		Key:                      ri.Get.Key,
		ProjectionExpression:     ri.Get.ProjectionExpression,
		ExpressionAttributeNames: ri.Get.ExpressionAttributeNames,
	}); err != nil {
		return nil, fmt.Errorf("failed to get item: %w", err)
	}

	if out.Item == nil {
		return emptyResult{}, nil
	}

	return newResult(out.Item), nil
}
