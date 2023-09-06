import boto3
import csv
import uuid 


def main():
    s3 = boto3.client('s3')
    dynamodb = boto3.client('dynamodb')
    bucket_name = 'employees-repo-ssmith'
    
    response = s3.list_objects_v2(Bucket=bucket_name)


    if 'Contents' in response:
    # Iterate through the object list
        for obj in response['Contents']:
            if 'processed' in obj['Key']:
                continue
            if 'Hold' in obj['Key']:
                continue
            print(obj)
            #print(obj)
            if '.csv' in obj['Key']:
                file_Name = obj['Key'][15::]
                #print(file_Name)
                response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                #print(response)
                #object_content = response['Body'].read().decode('utf-8')
                object_content = response['Body'].read().decode('utf-8').splitlines()
                lines = list(csv.reader(object_content, delimiter=','))
                if len(lines[0]) != 5:
                    s3.copy_object(
                    CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                    Bucket=bucket_name,
                    Key='employees-path/Hold/' + file_Name
                    )

                    # Delete the original object (optional)
                    s3.delete_object(
                    Bucket=bucket_name,
                    Key=obj['Key']
                    )
                    continue
                    
                if len(lines[0]) == 5 and lines[0][4] == 'E-mail':
                    lines.pop(0)
                    print(lines)
                    for line in lines:
                        new_item = {
                        'uuid': {'S': str(uuid.uuid1())},
                        'First Name': {'S': line[0]},
                        'Last Name': {'S': line[1]},
                        'Phone Number': {'S': line[2]},
                        'Company': {'S': line[3]},
                        'E-mail': {'S': line[4]}}
                        response = dynamodb.put_item(TableName='CUSTOMER_LEAD',Item=new_item)
                        #print(response)
                    s3.copy_object(
                    CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                    Bucket=bucket_name,
                    Key='employees-path/processed/' + file_Name
                    )

                    # Delete the original object (optional)
                    s3.delete_object(
                    Bucket=bucket_name,
                    Key=obj['Key']
                    )
                    
                    #print(f"First Name: {line[0]}")
                #file = csv.reader(object_content)
                #print(file)
                
                #for i in object_content:
                #    print(i)
                #print(f"csv file: {obj['Key']}")
    
if __name__ == '__main__':
    main()