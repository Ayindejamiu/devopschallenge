import os
import json
import boto3
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WeatherDashboard:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.s3_client = boto3.client('s3')
        self.kms_client = boto3.client('kms')
        self.kms_key_id = None

    def create_kms_key_if_not_exists(self):
        """Create a new KMS key if it doesn't exist"""
        try:
            # Attempt to fetch KMS Key ID from environment variable
            self.kms_key_id = os.getenv('AWS_KMS_KEY_ID')

            if not self.kms_key_id:
                print("Creating a new KMS key...")
                response = self.kms_client.create_key(
                    Description="Key for Weather Dashboard",
                    KeyUsage="ENCRYPT_DECRYPT",
                    CustomerMasterKeySpec="SYMMETRIC_DEFAULT",
                    Tags=[
                        {"TagKey": "Purpose", "TagValue": "WeatherDashboard"}
                    ]
                )
                self.kms_key_id = response['KeyMetadata']['KeyId']
                print(f"Created KMS key: {self.kms_key_id}")

                # Save the key ID to the environment variable
                os.environ['AWS_KMS_KEY_ID'] = self.kms_key_id
            else:
                print(f"Using existing KMS key: {self.kms_key_id}")
        except Exception as e:
            print(f"Error creating or using KMS key: {e}")

    def create_bucket_if_not_exists(self):
        """Create S3 bucket with default encryption if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} exists")
        except:
            print(f"Creating bucket {self.bucket_name}")
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"Successfully created bucket {self.bucket_name}")

                # Set default encryption for the bucket
                if self.kms_key_id:
                    self.s3_client.put_bucket_encryption(
                        Bucket=self.bucket_name,
                        ServerSideEncryptionConfiguration={
                            'Rules': [
                                {
                                    'ApplyServerSideEncryptionByDefault': {
                                        'SSEAlgorithm': 'aws:kms',
                                        'KMSMasterKeyID': self.kms_key_id
                                    }
                                }
                            ]
                        }
                    )
                    print(f"Default encryption set with KMS key {self.kms_key_id}")
                else:
                    print("Error: KMS key ID is not set. Bucket encryption skipped.")
            except Exception as e:
                print(f"Error creating bucket: {e}")

    def fetch_weather(self, city):
        """Fetch weather data from OpenWeather API"""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "imperial"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None

    def save_to_s3(self, weather_data, city):
        """Save weather data to S3 bucket with KMS encryption"""
        if not weather_data:
            return False
            
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        file_name = f"weather-data/{city}-{timestamp}.json"
        
        try:
            weather_data['timestamp'] = timestamp
            if not self.kms_key_id:
                raise ValueError("KMS Key ID is not set. Cannot save data with encryption.")

            print(f"Saving data to S3 with KMS key: {self.kms_key_id}")
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType='application/json',
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=self.kms_key_id
            )
            print(f"Successfully saved data for {city} to S3 with KMS encryption")
            return True
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()
    
    # Create KMS key if needed
    dashboard.create_kms_key_if_not_exists()

    # Create bucket if needed
    dashboard.create_bucket_if_not_exists()
    
    cities = ["Calgary", "Ontario", "New York"]
    
    for city in cities:
        print(f"\nFetching weather for {city}...")
        weather_data = dashboard.fetch_weather(city)
        if weather_data:
            temp = weather_data['main']['temp']
            feels_like = weather_data['main']['feels_like']
            humidity = weather_data['main']['humidity']
            description = weather_data['weather'][0]['description']
            
            print(f"Temperature: {temp}°F")
            print(f"Feels like: {feels_like}°F")
            print(f"Humidity: {humidity}%")
            print(f"Conditions: {description}")
            
            # Save to S3
            success = dashboard.save_to_s3(weather_data, city)
            if success:
                print(f"Weather data for {city} saved to S3!")
        else:
            print(f"Failed to fetch weather data for {city}")

if __name__ == "__main__":
    main()
