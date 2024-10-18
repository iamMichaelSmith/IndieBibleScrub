import json
import pdfplumber
import boto3
import re

# Initialize the S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SpotifyPlaylisters')  # Ensure the table name is correct

def extract_playlist_info(text, source_type="Spotify"):
    # Regex patterns to extract required information
    if source_type == "Spotify":
        playlists = re.split(r'\n(?=\#)', text)  # Assuming each playlist starts with a hashtag
    elif source_type == "YouTube":
        playlists = re.split(r'\n(?=\Owner)', text)  # For YouTube playlist owners

    for playlist in playlists:
        try:
            # Extract relevant fields using regex and provide default values if not found
            curator_name = re.search(r'(Curator|Owner):\s*(.*)', playlist)
            curator_name = curator_name.group(2).strip() if curator_name else ""

            email = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', playlist)
            email = email.group(0).strip() if email else ""

            location = re.search(r'Location:\s*(.*)', playlist)
            location = location.group(1).strip() if location else ""

            genres = re.search(r'Genres:\s*(.*)', playlist)
            genres = genres.group(1).strip() if genres else ""

            followers = re.search(r'Followers:\s*(\d+)', playlist)
            followers = followers.group(1).strip() if followers else "0"  # Default to "0"

            songs = re.search(r'Songs:\s*(\d+)', playlist)
            songs = songs.group(1).strip() if songs else "0"  # Default to "0"

            description = re.search(r'Description:\s*(.*)', playlist)
            description = description.group(1).strip() if description else ""

            website = re.search(r'Website:\s*(.*)', playlist)
            website = website.group(1).strip() if website else ""

            twitter = re.search(r'Twitter:\s*(.*)', playlist)
            twitter = twitter.group(1).strip() if twitter else ""

            spotify_page = re.search(r'Spotify Playlist Page:\s*(.*)', playlist)
            spotify_page = spotify_page.group(1).strip() if spotify_page else ""

            submission_method = re.search(r'Submission Method:\s*(.*)', playlist)
            submission_method = submission_method.group(1).strip() if submission_method else ""

            submission_page = re.search(r'Submission Page:\s*(.*)', playlist)
            submission_page = submission_page.group(1).strip() if submission_page else ""

            hashtags = re.findall(r'#\w+', playlist)

            # Only proceed if CuratorName and Email are present
            if curator_name and email:
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

                # Put the item into DynamoDB
                table.put_item(Item=item)

                print(f"Added playlist by {curator_name} to DynamoDB.")
            else:
                print(f"Skipping playlist due to missing key fields: {curator_name}, {email}")

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
