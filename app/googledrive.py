import io
import os.path
import pandas as pd

# ---- Google Drive API Imports ----
from google.oauth2 import service_account # Changed import for service account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

# If modifying these SCOPES, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_FILE = 'ucsd-hkn-hab-proj-cb2571e6851e.json' # Use the service account file


def authenticate():
    """
    Authenticates with the Google Drive API using OAuth 2.0.
    Handles token refresh and saving for persistent access.

    Returns:
        googleapiclient.discovery.Resource: An authorized Google Drive service instance.
    """
    creds = None
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account key file not found at {SERVICE_ACCOUNT_FILE}")

    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    except Exception as e:
        print(f"Error loading credentials from service account file: {e}")
        return None
    try:
        service = build('drive', 'v3', credentials=creds)
        print("Google Drive API service created successfully.")
        return service
    except Exception as e:
        print(f"Error creating Google Drive service: {e}")
        return None

def get_folder_id(drive_service, folder_name):
    """Finds the ID of a folder by name."""
    try:
        # Escape single quotes in the folder name for the query
        escaped_folder_name = folder_name.replace("'", "\\'")
        results = drive_service.files().list(
            q=f"name='{escaped_folder_name}' and mimeType='application/vnd.google-apps.folder'",
            spaces='drive',
            fields='files(id, name)').execute()
        items = results.get('files', [])
        if not items:
            print(f"Folder not found: {folder_name}")
            return None
        # Assuming the first match is the correct one
        print(f"Found folder '{folder_name}' with ID: {items[0]['id']}")
        return items[0]['id']
    except HttpError as error:
        print(f"An error occurred finding the folder: {error}")
        return None


def download_csv(drive_service, file_name, folder_name=None):
    '''
        
    '''
    try:
        folder_id = None
        if folder_name:
            folder_id = get_folder_id(drive_service, folder_name)
            if not folder_id:
                return None  # Folder not found or error occurred

        print('folder id:', folder_id)
        query = f"name='{file_name}' and mimeType='text/csv'"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        print('query', query)

        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)').execute()

        items = results.get('files', [])
        if not items:
            print(f"File not found: {file_name}")
            return None

        file_id = items[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        print(f"Downloading '{file_name}'...")
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%.")

        fh.seek(0)
        print("Download complete.")
        return fh

    except HttpError as error:
        print(f"An error occurred downloading the file: {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

    
def main():       
    auth = authenticate()
    print(download_csv(auth, "Cdata_w_gaps_and_wind.csv"))

if __name__ == "__main__":
    main()