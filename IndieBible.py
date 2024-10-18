import json
import pdfplumber
import boto3
import re
import time

# Initialize the S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SpotifyPlaylisters')  # Ensure the table name is correct

# Mapping of potential keywords to their respective fields
FIELD_MAPPING = {
    'Curator': 'CuratorName',
    'Owner': 'CuratorName',
    'Email': 'Email',
    'Location': 'Location',
    'Genres': 'Genres',
    'Followers': 'Followers',
    'Songs': 'Songs',
    'Description': 'Description',
    'Website': 'Website',
    'Twitter': 'Twitter',
    'Spotify Playlist Page': 'SpotifyPlaylistPage',
    'Submission Method': 'SubmissionMethod',
    'Submission Page': 'SubmissionPage'
}

def extract_field(pattern, playlist, default_value=""):
    """Helper function to extract a field from the playlist using regex."""
    match = re.search(pattern, playlist)
    return match.group(1).strip() if match else default_value

def extract_playlist_info(text, source_type="Spotify"):
    # Regex patterns to extract required information
    if source_type == "Spotify":
        playlists = re.split(r'\n(?=\#)', text)  # Assuming each playlist starts with a hashtag
    elif source_type == "YouTube":
        playlists = re.split(r'\n(?=\Owner)', text)  # For YouTube playlist owners

    for playlist in playlists:
        try:
            # Extract relevant fields using the FIELD_MAPPING
            curator_name = extract_field(r'(Curator|Owner):\s*(.*)', playlist, "Unknown Curator")
            email = extract_field(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', playlist, "no-email@example.com")
            location = extract_field(r'Location:\s*(.*)', playlist)
            genres = extract_field(r'Genres:\s*(.*)', playlist)
            followers = extract_field(r'Followers:\s*(\d+)', playlist, "0")  # Default to "0"
            songs = extract_field(r'Songs:\s*(\d+)', playlist, "0")  # Default to "0"
            description = extract_field(r'Description:\s*(.*)', playlist)
            website = extract_field(r'Website:\s*(.*)', playlist)
            twitter = extract_field(r'Twitter:\s*(.*)', playlist)
            spotify_page = extract_field(r'Spotify Playlist Page:\s*(.*)', playlist)
            submission_method = extract_field(r'Submission Method:\s*(.*)', playlist)
            submission_page = extract_field(r'Submission Page:\s*(.*)', playlist)
            hashtags = re.findall(r'#\w+', playlist)

            # Prepare the item for DynamoDB
            item = {
                'CuratorName': curator_name,
                'Email': email,
                'Location': location,
                'Genres': list(set(genres.split(', '))) if genres else [],  # Convert to list or leave empty
                'Followers': int(followers),
                'Songs': int(songs),
                'Description': description,
                'Website': website,
                'Twitter': twitter,
                'SpotifyPlaylistPage': spotify_page,
                'SubmissionMethod': submission_method,
                'SubmissionPage': submission_page,
                'Hashtags': hashtags if hashtags else []  # Leave empty if no hashtags found
            }

            # Use batch writer to put items
            with table.batch_writer() as batch:
                try:
                    batch.put_item(Item=item)
                    print(f"Added playlist by {curator_name} to DynamoDB.")
                except Exception as db_error:
                    print(f"Error adding playlist by {curator_name} to DynamoDB: {db_error}")

            # Introduce a delay to prevent throttling
            time.sleep(0.1)  # Adjust the sleep time as needed

        except Exception as e:
            print(f"Error processing playlist: {e}")

def process_pdf_from_s3(bucket_name, file_type="Spotify"):
    # List all objects in the specified S3 bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    
    # Check if the response contains 'Contents'
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            # Process only PDF files
            if key.endswith('.pdf'):
                # Download the PDF from S3 to the local instance
                s3_client.download_file(bucket_name, key, f'/tmp/{key.split("/")[-1]}')

                # Read and parse the PDF
                with pdfplumber.open(f'/tmp/{key.split("/")[-1]}') as pdf:
                    for page in pdf.pages:
                        try:
                            text = page.extract_text()
                            if text:
                                extract_playlist_info(text, source_type=file_type)
                        except Exception as e:
                            print(f"Error reading page {page.page_number}: {e}")

if __name__ == "__main__":
    bucket_name = 'indie-bible-bucket'  # Your S3 bucket name
    process_pdf_from_s3(bucket_name, file_type="Spotify")  # Pass 'YouTube' for YouTube PDFs
