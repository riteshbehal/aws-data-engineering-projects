# Zip the python script for lambda 
zip function.zip lambda_function.py

# Deploy Lambda function
aws lambda create-function --function-name test-lambda \
--zip-file fileb://function.zip \
--handler lambda_function.lambda_handler \
--runtime python3.11 \
--role arn:aws:iam::463646775279:role/service-role/aws-lambda-dynamodb1-role-htl5qqed

# AWS Glue deployment

# Copy python script to S3
aws s3 cp mysql_extraction.py s3://ab-aws-de-labs/glue-scripts/

# Deploy glue job using the uploaded script from S3 
aws glue create-job --name "mysql-extraction-job" --role "arn:aws:iam::463646775279:role/custom-glue-role" \
--command '{"Name":"deploy-mysql-extraction", "ScriptLocation":"s3://ab-aws-de-labs/glue-scripts/mysql-extraction.py", "PythonVersion":"3"}' --glue-version "4.0" --max-capacity 1
