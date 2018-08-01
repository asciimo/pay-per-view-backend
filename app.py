import boto3
import json
import os
import logging

s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def respond(err, res=None):
    return {
        'statusCode': 400 if err else 200,
        'body': json.dumps({"error": err}) if err else json.dumps({"result": res})
    }


def validate_secret(parameters, key, value):
    if key in parameters.keys() and parameters[key] == value:
        return True
    return False


def transform_to_csv(body):
    """ @todo: turn body into a CSV """
    logger.info('Got this POSTed: {}'.format(body))
    return body


def get_csv_filename(body):
    """ @todo: create a filename like YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS-installation_id.csv """
    logger.info('Got this POSTed: {}'.format(body))
    return body


def lambda_handler(event, context):
    if event['httpMethod'] != 'POST' or not event['body']:
        logger.error('Received an invalid POST request.')
        return respond('Not a valid POST request.')

    if not validate_secret(event['queryStringParameters'],
                           os.environ['request_secret_key'],
                           os.environ['request_secret_value']):
        logger.error('Secret invalid.')
        return respond('Invalid request.')

    logger.info('Transforming body...')
    csv = transform_to_csv(event['body'])
    filename = get_csv_filename(event['body'])
    logger.info('Trying to write to S3...')
    result = s3.put_object(Body=csv, Bucket=os.environ['btl_data_bucket'],
                           Key='test2.json')

    if result['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error('Problem saving to S3: {}'.format(result))
        return respond('Error while storing data.')

    return respond(None, 'OK')