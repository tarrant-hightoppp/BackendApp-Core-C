"""  """import os
import sys
import io

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.s3 import S3Service
from app.utils.minio_init import init_minio_bucket
from app.core.config import settings

def test_bucket_initialization():
    """Test that the MinIO bucket is properly initialized"""
    print("\n=== Testing MinIO Bucket Initialization ===")
    
    # Initialize the bucket
    result = init_minio_bucket()
    print(f"Bucket initialization result: {result}")
    
    if not result:
        print("❌ Bucket initialization failed")
        return False
    
    print("✅ Bucket initialization successful")
    return True

def test_file_upload():
    """Test uploading a file to the MinIO bucket"""
    print("\n=== Testing File Upload to MinIO ===")
    
    # Create S3 service
    s3_service = S3Service()
    
    # Create a test file
    test_content = io.BytesIO(b"This is a test file for MinIO upload")
    test_filename = "test_upload.txt"
    
    # Upload the file
    success, message = s3_service.upload_file(test_content, test_filename)
    print(f"Upload result: {success}, Message: {message}")
    
    if not success:
        print("❌ File upload failed")
        return False
    
    print("✅ File upload successful")
    
    # Try to download the file to verify
    try:
        content = s3_service.download_file(test_filename)
        if content:
            print(f"Downloaded content: {content.decode('utf-8')}")
            print("✅ File download successful")
            return True
        else:
            print("❌ File download failed")
            return False
    except Exception as e:
        print(f"❌ Error downloading file: {e}")
        return False

def test_directory_creation():
    """Test creating a directory structure in the MinIO bucket"""
    print("\n=== Testing Directory Creation in MinIO ===")
    
    # Create S3 service
    s3_service = S3Service()
    
    # Create a test file in a nested directory
    test_content = io.BytesIO(b"This is a test file in a nested directory")
    test_filename = "uploaded_files/test_dir/nested_test.txt"
    
    # Upload the file
    success, message = s3_service.upload_file(test_content, test_filename)
    print(f"Upload result: {success}, Message: {message}")
    
    if not success:
        print("❌ Directory creation failed")
        return False
    
    print("✅ Directory creation successful")
    return True

if __name__ == "__main__":
    print(f"Testing MinIO connection with bucket: {settings.S3_BUCKET}")
    print(f"MinIO endpoint: {settings.S3_ENDPOINT_URL}")
    
    # Run tests
    bucket_init_success = test_bucket_initialization()
    
    if bucket_init_success:
        file_upload_success = test_file_upload()
        directory_success = test_directory_creation()
        
        if file_upload_success and directory_success:
            print("\n✅ All tests passed successfully!")
        else:
            print("\n❌ Some tests failed")
    else:
        print("\n❌ Bucket initialization failed, skipping other tests")