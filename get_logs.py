#!/usr/bin/env python

import boto3
import boto3.session
import s3fs
import os
import datetime as dt
from dateutil.tz import tzutc
import logging
import argparse

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(
    description='Download logs of AWS datapipeline.'
)
parser.add_argument('name', help='Name of the datapipeline you want '
                                 'to get logs of.')
parser.add_argument('--logdir',
                    help='General s3 location for datapipelines logs '
                         '(look for pipelineLogUri) (may be stored in '
                         'DATAPIPELINE_LOGDIR env var).',
                    default=os.environ.get('DATAPIPELINE_LOGDIR',
                                           default=None)
                    )
parser.add_argument('--profile',
                    help='AWS profile to use (may be stored in '
                         'AWS_PROFILE env var).',
                    default=os.environ.get('AWS_PROFILE', default=None))
parser.add_argument('--region',
                    help='AWS region to use (may be stored in '
                         'AWS_REGION env var)',
                    default=os.environ.get('AWS_REGION', default=None))
parser.add_argument('--date',
                    help='Date in format YYYY-MM-DD. If not specified, '
                         'newest logs will be downloaded.')


def find_dir(path, fs, date=None):
    walk = fs.walk(path)
    dirs = {os.path.dirname(item) for item in walk}
    max_date = dt.datetime(1990, 1, 1, tzinfo=tzutc())
    newest_file = None
    for dir_ in dirs:
        files = fs.ls(dir_, detail=True)
        for file in files:
            if date is not None and file['LastModified'].date() == date:
                newest_file = file['Key']
                break
            if file['LastModified'] > max_date:
                newest_file = file['Key']
                max_date = file['LastModified']
    newest_dir = os.path.dirname(newest_file)
    return newest_dir


def get_all_pipelines(client):
    res = client.list_pipelines()
    pipeline_ids = res['pipelineIdList']
    while res['hasMoreResults']:
        res = client.list_pipelines(marker=res['marker'])
        pipeline_ids.extend(res['pipelineIdList'])
    return pipeline_ids


def find_pipeline(pipelines, name):
    pipeline_id = None
    for pipeline_dict in pipelines:
        if pipeline_dict['name'] == name:
            pipeline_id = pipeline_dict['id']
            break
    if pipeline_id is None:
        logging.info('Available pipelines: ')
        logging.info([pipeline['name'] for pipeline in pipelines])
        raise RuntimeError("No such pipeline.")
    return pipeline_id


def main(name, s3_bucket, profile_name, region_name, date):
    client = boto3.session \
        .Session(profile_name=profile_name, region_name=region_name) \
        .client('datapipeline')
    fs = s3fs.S3FileSystem(profile_name=profile_name, region_name=region_name)

    pipelines = get_all_pipelines(client)
    pipeline_id = find_pipeline(pipelines, name)
    description = client.describe_pipelines(pipelineIds=[pipeline_id])
    logging.info('Found pipeline with id {}'.format(pipeline_id))
    pipeline_s3_dir = os.path.join(s3_bucket, pipeline_id)
    newest_dir = find_dir(pipeline_s3_dir, fs, date=date)
    logging.info('Newest dir is {}'.format(os.path.basename(newest_dir)))
    for file in fs.ls(newest_dir):
        filename = os.path.basename(file)
        fs.get(file, filename)
        logging.info('Downloaded file {}'.format(filename))


if __name__ == '__main__':
    args = parser.parse_args()
    if args.date is not None:
        args.date = dt.datetime.strptime(args.date, '%Y-%m-%d').date()
    main(args.name, args.logdir, args.profile, args.region, args.date)
