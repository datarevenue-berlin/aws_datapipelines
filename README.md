# Quickly get logs of AWS Datapipeline

Usage: `python get_logs.py --name <pipeline_name> 
--logdir <logs_directory_on_s3> --profile <aws_profile> --region <aws_region>`

This will connect with `datapipeline` and `s3` services using specified profile
and region, 
search for the ID of pipeline with specified name,
search for this pipeline's logs directory on s3 
and download logs of the latest run.

Optional arguments can be read from env variables (see help).

Useful for those who don't have access to the logs via web browser.

Hint: logs directory is specified at datapipeline creation with 
`pipelineLogUri` in the default object in SDK/CLI 
([reference](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-viewing-logs.html)).

