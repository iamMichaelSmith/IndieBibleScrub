import json
import pdfplumber
import boto3
import re

# Initialize the S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SpotifyPlaylisters')  # Change this to your DynamoDB table name

def extract_playlist_info(text):
    # Regex patterns to extract required information
    playlists = re.split(r'\n(?=\#)', text)  # Assuming each playlist starts with a hashtag

    for playlist in playlists:
        try:
            # Extract relevant fields using regex
            curator_name = re.search(r'Curator:\s*(.*)', playlist).group(1).strip()
            email = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', playlist)
            email = email.group(0).strip() if email else None
            location = re.search(r'Location:\s*(.*)', playlist)
            location = location.group(1).strip() if location else None
            genres = re.search(r'Genres:\s*(.*)', playlist).group(1).strip()
            followers = re.search(r'Followers:\s*(\d+)', playlist).group(1).strip()
            songs = re.search(r'Songs:\s*(\d+)', playlist).group(1).strip()
            description = re.search(r'Description:\s*(.*)', playlist).group(1).strip()
            website = re.search(r'Website:\s*(.*)', playlist)
            website = website.group(1).strip() if website else None
            twitter = re.search(r'Twitter:\s*(.*)', playlist)
            twitter = twitter.group(1).strip() if twitter else None
            spotify_page = re.search(r'Spotify Playlist Page:\s*(.*)', playlist).group(1).strip()
            submission_method = re.search(r'Submission Method:\s*(.*)', playlist)
            submission_method = submission_method.group(1).strip() if submission_method else None
            submission_page = re.search(r'Submission Page:\s*(.*)', playlist)
            submission_page = submission_page.group(1).strip() if submission_page else None
            hashtags = re.findall(r'#\w+', playlist)

            # Prepare the item for DynamoDB
            item = {
                'CuratorName': curator_name,
                'Email': email,
                'Location': location,
                'Genres': list(set(genres.split(', '))),  # Convert string to list
                'Followers': int(followers),
                'Songs': int(songs),
                'Description': description,
                'Website': website,
                'Twitter': twitter,
                'SpotifyPlaylistPage': spotify_page,
                'SubmissionMethod': submission_method,
                'SubmissionPage': submission_page,
                'Hashtags': hashtags
            }

            # Put the item into DynamoDB
            table.put_item(Item=item)

            print(f"Added playlist by {curator_name} to DynamoDB.")

        except Exception as e:
            print(f"Error processing playlist: {e}")

def process_pdf_from_s3(bucket_name, file_key):
    # Download the PDF from S3 to the local instance
    s3_client.download_file(bucket_name, file_key, '/tmp/indie_spotify_bible.pdf')

    # Read and parse the PDF
    with pdfplumber.open('/tmp/indie_spotify_bible.pdf') as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                extract_playlist_info(text)

if __name__ == "__main__":
    bucket_name = 'indie-bible-bucket'  # Your S3 bucket name
    file_key = 'INDIE-SPOTIFY-BIBLE.pdf'  # The key of the PDF file in the S3 bucket

    process_pdf_from_s3(bucket_name, file_key)
