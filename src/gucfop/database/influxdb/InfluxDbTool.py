from influxdb_client import InfluxDBClient
from influxdb_client import PostBucketRequest
from influxdb_client.rest import ApiException

class InfluxDbTool():

    #
    # constructor
    #
    def __init__(
            self,
            INFLUXDB_URL : str,
            INFLUXDB_TOKEN : str,
            INFLUXDB_ORG : str,
    ):
        self.INFLUXDB_URL = INFLUXDB_URL
        self.INFLUXDB_TOKEN = INFLUXDB_TOKEN
        self.INFLUXDB_ORG = INFLUXDB_ORG
        self.client = InfluxDBClient(url=self.INFLUXDB_URL, token=self.INFLUXDB_TOKEN, org=self.INFLUXDB_ORG)

    #
    # deconstructor
    #
    def __del__(self):
        self.client.close()

    #
    # create a bucket
    #
    def create_bucket(self, bucket_name : str):
        buckets_api = self.client.buckets_api()
        org = self.client.organizations_api().find_organizations(org = self.INFLUXDB_ORG)[0]
        org_id = org.id
    
        try:
            # Check if the bucket already exists
            existing_bucket = buckets_api.find_bucket_by_name(bucket_name)
            if existing_bucket:
                print(f"Bucket '{bucket_name}' already exists.")
            else:
                # Create a new bucket
                bucket_req = PostBucketRequest(
                    org_id = org_id,
                    name = bucket_name,
                )
                bucket = buckets_api.create_bucket(bucket = bucket_req)
                print("Created bucket:", bucket.name, "schema_type:", bucket.schema_type)
        
        except ApiException as e:
            print(f"Error creating bucket: {e}")

    #
    # delete a bucket
    #
    def delete_bucket(self, bucket_name -> str):
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        try:
            # Find the target bucket
            target_bucket = None
            for bucket in buckets:
                if bucket.name == bucket_name:
                    target_bucket = bucket
                    buckets_api.delete_bucket(target_bucket.id)
                    print(f"Bucket '{bucket_name}' deleted successfully.")
                    break
            
            if not target_bucket:
                print(f"Bucket '{bucket_name}' not found.")
        except ApiException as e:
            print(f"Error deleting bucket: {e}")


