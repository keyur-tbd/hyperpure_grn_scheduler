#!/usr/bin/env python3
"""
Hyperpure GRN Scheduler - Runs workflows every 3 hours and logs to Google Sheets
"""

import os
import json
import base64
import tempfile
import time
import logging
import schedule
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
import warnings

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import io

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False
    print("WARNING: llama_cloud_services not available")

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hyperpure_automation.log'),
        logging.StreamHandler()
    ]
)

# Hardcoded configuration for Hyperpure
CONFIG = {
    'mail': {
        'sender': 'noreply@hyperpure.com',
        'search_term': 'GRN against PO Number',
        'gdrive_folder_id': '1euqxO-meY4Ahszpdk3XbwlRwvkfSlY8k',
        'attachment_filter': 'attachment.pdf',
        'days_back': 7,
        'max_results': 1000
    },
    'sheet': {
        'llama_api_key': 'llx-csECp5RB25AeiLp57MQ8GnpViLFNyaezTOoHQIiwD7yn0CMr',
        'llama_agent': 'Hyperpure Agent',
        'drive_folder_id': '1aUjRMqWjVDDAsQw0TugwgmwYjxP6W7DT',
        'spreadsheet_id': '1B1C2ILnIMXpEYbQzaSkhRzEP2gmgE2YLRNqoX98GwcU',
        'sheet_range': 'hyperpuregrn',
        'days_back': 7,
        'max_files': 1000,
        'failed_extractions_sheet': 'failed_extractions'
    },
    'workflow_log': {
        'spreadsheet_id': '1B1C2ILnIMXpEYbQzaSkhRzEP2gmgE2YLRNqoX98GwcU',
        'sheet_range': 'workflow_logs'
    },
    'remaining_files': {
        'spreadsheet_id': '1B1C2ILnIMXpEYbQzaSkhRzEP2gmgE2YLRNqoX98GwcU',
        'sheet_range': 'remaining_files'
    },
    'notifications': {
        'recipients': ['keyur@thebakersdozen.in'],  # Add your email here
        'sender_email': 'blinkit@thebakersdozen.in'  # Will be auto-populated from authenticated user
    },
    'credentials_path': 'credentials.json',
    'token_path': 'token.json'
}


class HyperpureAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes - Added gmail.send scope for email notifications
        self.gmail_scopes = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def log(self, message: str, level: str = "INFO"):
        """Log message with appropriate level"""
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)
    
    def authenticate(self):
        """Authenticate using local credentials file"""
        try:
            self.log("Starting authentication process...", "INFO")
            
            creds = None
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            # Load token if exists
            if os.path.exists(CONFIG['token_path']):
                creds = Credentials.from_authorized_user_file(CONFIG['token_path'], combined_scopes)
            
            # Refresh or get new credentials
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    self.log("Refreshing expired token...", "INFO")
                    creds.refresh(Request())
                else:
                    if not os.path.exists(CONFIG['credentials_path']):
                        self.log(f"Credentials file not found: {CONFIG['credentials_path']}", "ERROR")
                        return False
                    
                    self.log("Starting new OAuth flow...", "INFO")
                    flow = InstalledAppFlow.from_client_secrets_file(
                        CONFIG['credentials_path'], combined_scopes)
                    creds = flow.run_local_server(port=0)
                
                # Save credentials
                with open(CONFIG['token_path'], 'w') as token:
                    token.write(creds.to_json())
                self.log("Token saved successfully", "INFO")
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            # Get authenticated user's email for sender
            try:
                profile = self.gmail_service.users().getProfile(userId='me').execute()
                CONFIG['notifications']['sender_email'] = profile['emailAddress']
                self.log(f"Authenticated as: {profile['emailAddress']}", "INFO")
            except Exception as e:
                self.log(f"Could not get user profile: {str(e)}", "WARNING")
            
            self.log("Authentication successful!", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False
    
    def send_email_notification(self, summary_data: dict):
        """Send email notification with workflow summary"""
        try:
            self.log("Preparing email notification...", "INFO")
            
            # Get sender email from authenticated user
            sender_email = CONFIG['notifications']['sender_email']
            
            # Create email body
            subject = f"Hyperpure GRN Scheduler Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Format the email body
            body_lines = [
                "HYPERPURE GRN SCHEDULER WORKFLOW SUMMARY",
                "=" * 50,
                "",
                f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"Days Back Parameter: {CONFIG['mail']['days_back']} days",
                "",
                "MAIL TO DRIVE WORKFLOW:",
                f"  • Sender Filter: {CONFIG['mail']['sender']}",
                f"  • Search Term: {CONFIG['mail']['search_term']}",
                f"  • Number of emails processed: {summary_data.get('mail_emails_processed', 0)}",
                f"  • Number of attachments uploaded: {summary_data.get('mail_attachments_uploaded', 0)}",
                f"  • Failed to upload: {summary_data.get('mail_upload_failed', 0)}",
                "",
                "DRIVE TO SHEET WORKFLOW:",
                f"  • Number of files found (last {CONFIG['sheet']['days_back']} days): {summary_data.get('drive_files_found', 0)}",
                f"  • Number of files skipped (already processed): {summary_data.get('drive_files_skipped', 0)}",
                f"  • Number of files successfully processed: {summary_data.get('drive_files_processed', 0)}",
                f"  • Number of files failed to process: {summary_data.get('drive_files_failed', 0)}",
                f"  • Incomplete extractions: {summary_data.get('drive_incomplete_extractions', 0)}",
                f"  • Total rows added to sheet: {summary_data.get('drive_rows_added', 0)}",
                "",
                "OVERALL STATUS:",
                f"  • Total Duration: {summary_data.get('total_duration', '0s')}",
                f"  • Workflow Status: {'SUCCESS' if summary_data.get('overall_success', False) else 'PARTIAL SUCCESS' if summary_data.get('any_success', False) else 'FAILED'}",
                "",
                "=" * 50,
                "This is an automated report from Hyperpure GRN Scheduler.",
                ""
            ]
            
            email_body = "\n".join(body_lines)
            
            # Create message
            message = self.create_email_message(
                sender=sender_email,
                to=CONFIG['notifications']['recipients'],
                subject=subject,
                body_text=email_body
            )
            
            # Send email
            sent_message = self.gmail_service.users().messages().send(
                userId='me',
                body=message
            ).execute()
            
            self.log(f"Email notification sent successfully! Message ID: {sent_message['id']}", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Failed to send email notification: {str(e)}", "ERROR")
            return False
    
    def create_email_message(self, sender: str, to: list, subject: str, body_text: str) -> dict:
        """Create an email message in Gmail format"""
        # Create email headers
        message_parts = [
            f"From: {sender}",
            f"To: {', '.join(to)}",
            f"Subject: {subject}",
            "Content-Type: text/plain; charset=utf-8",
            "MIME-Version: 1.0",
            "",
            body_text
        ]
        
        message = "\n".join(message_parts)
        
        # Encode message in base64
        encoded_message = base64.urlsafe_b64encode(message.encode("utf-8")).decode("utf-8")
        
        return {
            'raw': encoded_message
        }
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')  
            
            if search_term:
                query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Gmail search query: {query}", "INFO")
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"Found {len(messages)} emails matching criteria", "INFO")
            
            return messages
            
        except Exception as e:
            self.log(f"Gmail search failed: {str(e)}", "ERROR")
            return []
    
    def get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"Failed to get email details for {message_id}: {str(e)}", "ERROR")
            return {}
    
    def find_target_folder(self, parent_folder_id: str) -> Optional[str]:
        """Find the PDFs folder in Gmail_Attachments/Hyperpure GRN/PDFs/"""
        try:
            # Find Gmail_Attachments folder
            query = f"name='Gmail_Attachments' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            result = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = result.get('files', [])
            if not files:
                self.log("Gmail_Attachments folder not found", "ERROR")
                return None
            gmail_folder_id = files[0]['id']
            
            # Find Hyperpure GRN folder
            query = f"name='Hyperpure GRN' and mimeType='application/vnd.google-apps.folder' and '{gmail_folder_id}' in parents and trashed=false"
            result = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = result.get('files', [])
            if not files:
                self.log("Hyperpure GRN folder not found", "ERROR")
                return None
            hyperpure_folder_id = files[0]['id']
            
            # Find PDFs folder
            query = f"name='PDFs' and mimeType='application/vnd.google-apps.folder' and '{hyperpure_folder_id}' in parents and trashed=false"
            result = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = result.get('files', [])
            if not files:
                self.log("PDFs folder not found", "ERROR")
                return None
            return files[0]['id']
        except Exception as e:
            self.log(f"Failed to find target folder: {str(e)}", "ERROR")
            return None
    
    def upload_to_drive(self, file_data: bytes, filename: str, folder_id: str, message_id: str) -> bool:
        """Upload file to Google Drive with message ID prefix"""
        try:
            # Use the original filename but add message ID prefix
            prefixed_filename = f"{message_id}_{filename}"
            
            # Check if file already exists
            query = f"name='{prefixed_filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            if existing.get('files', []):
                self.log(f"File already exists, skipping: {prefixed_filename}", "INFO")
                return True
                
            # Upload the file
            file_metadata = {
                'name': prefixed_filename,
                'parents': [folder_id]
            }
            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype='application/pdf',
                resumable=True
            )
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            self.log(f"Uploaded to Drive: {prefixed_filename}", "SUCCESS")
            return True
        except Exception as e:
            self.log(f"Failed to upload {prefixed_filename}: {str(e)}", "ERROR")
            return False
    
    def process_attachment(self, message_id: str, part: Dict, folder_id: str) -> bool:
        try:
            filename = part.get("filename", "").lower()
            if filename != CONFIG['mail']['attachment_filter'].lower():
                return False
            att_id = part["body"].get("attachmentId")
            if not att_id:
                return False
            att = self.gmail_service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=att_id
            ).execute()
            file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
            return self.upload_to_drive(file_data, filename, folder_id, message_id)
        except Exception as e:
            self.log(f"Failed to process attachment for message {message_id}: {str(e)}", "ERROR")
            return False
    
    def extract_attachments_from_email(self, message_id: str, payload: Dict, folder_id: str) -> int:
        count = 0
        if "parts" in payload:
            for part in payload["parts"]:
                count += self.extract_attachments_from_email(message_id, part, folder_id)
        if "filename" in payload and "attachmentId" in payload.get("body", {}):
            if self.process_attachment(message_id, payload, folder_id):
                count += 1
        return count
    
    def process_mail_to_drive_workflow(self, config: dict):
        """Process Mail to Drive workflow for Hyperpure"""
        try:
            self.log("Starting Gmail to Drive workflow", "INFO")
            
            # Find the target PDFs folder
            target_folder_id = self.find_target_folder(config['gdrive_folder_id'])
            if not target_folder_id:
                self.log("Target folder structure not found", "ERROR")
                return {'success': False, 'processed': 0, 'total_attachments': 0, 'failed': 0}
            
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {'success': True, 'processed': 0, 'total_attachments': 0, 'failed': 0}
            
            self.log(f"Found {len(emails)} emails. Processing attachments...", "INFO")
            
            processed_count = 0
            failed_count = 0
            for i, email in enumerate(emails):
                self.log(f"Processing email {i+1}/{len(emails)}", "INFO")
                try:
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id']
                    ).execute()
                    att_count = self.extract_attachments_from_email(email['id'], message['payload'], target_folder_id)
                    if att_count > 0:
                        processed_count += att_count
                    else:
                        failed_count += 1
                except Exception as e:
                    self.log(f"Failed to process email {email['id']}: {str(e)}", "ERROR")
                    failed_count += 1
            
            self.log(f"Gmail workflow completed. Processed {processed_count} attachments", "SUCCESS")
            return {
                'success': True, 
                'processed': processed_count, 
                'total_attachments': len(emails), 
                'failed': failed_count,
                'emails_processed': len(emails)
            }
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0, 'total_attachments': 0, 'failed': 0, 'emails_processed': 0}
    
    def list_drive_pdfs(self, folder_id: str, days_back: int = 1, all_time: bool = False) -> List[Dict]:
        try:
            if all_time:
                query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
            else:
                start_datetime = datetime.utcnow() - timedelta(days=days_back - 1)
                start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
                query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime >= '{start_str}'"
            files = []
            page_token = None
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, createdTime)",
                    orderBy="createdTime desc",
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            self.log(f"Found {len(files)} PDF files in folder ({'all time' if all_time else f'last {days_back} days'})", "INFO")
            return files
        except Exception as e:
            self.log(f"Failed to list PDFs: {str(e)}", "ERROR")
            return []
    
    def download_from_drive(self, file_id: str, file_name: str) -> bytes:
        try:
            self.log(f"Downloading: {file_name}", "INFO")
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            self.log(f"Downloaded: {file_name}", "SUCCESS")
            return file_data
        except Exception as e:
            self.log(f"Failed to download {file_name}: {str(e)}", "ERROR")
            return b""
    
    def get_sheet_data(self, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
        """Get all data from the sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                majorDimension="ROWS"
            ).execute()
            return result.get('values', [])
        except Exception as e:
            self.log(f"Failed to get sheet data: {str(e)}", "ERROR")
            return []
    
    def get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Get the numeric sheet ID for the given sheet name"""
        try:
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for sheet in metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            self.log(f"Sheet '{sheet_name}' not found", "ERROR")
            return 0
        except Exception as e:
            self.log(f"Failed to get sheet metadata: {str(e)}", "ERROR")
            return 0
    
    def get_existing_drive_ids(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "drive_file_id" not in headers:
                self.log("No 'drive_file_id' column found in sheet", "WARNING")
                return set()
            id_index = headers.index("drive_file_id")
            existing_ids = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            self.log(f"Found {len(existing_ids)} existing file IDs in sheet", "INFO")
            return existing_ids
        except Exception as e:
            self.log(f"Failed to get existing file IDs: {str(e)}", "ERROR")
            return set()
    
    def get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            self.log(f"No existing headers or error: {str(e)}")
            return []
    
    def update_headers(self, spreadsheet_id: str, sheet_name: str, new_headers: List[str]) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [new_headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + len(new_headers))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.log(f"Updated headers with {len(new_headers)} columns")
            return True
        except Exception as e:
            self.log(f"Failed to update headers: {str(e)}")
            return False
    
    def append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, 
                    range=range_name,
                    valueInputOption='USER_ENTERED', 
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                self.log(f"Appended {updated_cells} cells to Google Sheet")
                return True
            except Exception as e:
                if attempt < max_retries:
                    self.log(f"Attempt {attempt} failed: {str(e)}")
                    time.sleep(wait_time)
                else:
                    self.log(f"Failed to append to Google Sheet: {str(e)}")
                    return False
        return False
    
    def replace_rows_for_file(self, spreadsheet_id: str, sheet_name: str, file_id: str, 
                              headers: List[str], new_rows: List[List[Any]], sheet_id: int) -> bool:
        """Delete existing rows for the file if any, and append new rows"""
        try:
            values = self.get_sheet_data(spreadsheet_id, sheet_name)
            if not values:
                return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            
            current_headers = values[0]
            data_rows = values[1:]
            
            try:
                file_id_col = current_headers.index('drive_file_id')
            except ValueError:
                self.log("No 'drive_file_id' column found, appending new rows", "INFO")
                return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            
            rows_to_delete = []
            for idx, row in enumerate(data_rows, 2):
                if len(row) > file_id_col and row[file_id_col] == file_id:
                    rows_to_delete.append(idx)
            
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)
                requests = []
                for row_idx in rows_to_delete:
                    requests.append({
                        'deleteDimension': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,
                                'endIndex': row_idx
                            }
                        }
                    })
                body = {'requests': requests}
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=body
                ).execute()
                self.log(f"Deleted {len(rows_to_delete)} existing rows for file {file_id}", "INFO")
            
            return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
        except Exception as e:
            self.log(f"Failed to replace rows: {str(e)}", "ERROR")
            return False
    
    def process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        rows = []
        items = []
        if "items" in extracted_data:
            items = extracted_data["items"]
        elif "product_items" in extracted_data:
            items = extracted_data["product_items"]
        else:
            self.log(f"No recognizable items key in {file_info['name']}", "WARNING")
            return rows
        for item in items:
            item["po_number"] = extracted_data.get("po_number") or extracted_data.get("purchase_order_number") or ""
            item["vendor_invoice_number"] = extracted_data.get("vendor_invoice_number") or extracted_data.get("invoice_number") or extracted_data.get("supplier_bill_number") or ""
            item["supplier"] = extracted_data.get("supplier") or extracted_data.get("vendor") or ""
            item["shipping_address"] = extracted_data.get("shipping_address") or extracted_data.get("receiver_address") or extracted_data.get("Shipping Address") or ""
            item["grn_date"] = extracted_data.get("grn_date") or extracted_data.get("delivered_on") or ""
            item["source_file"] = file_info['name']
            item["processed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item["drive_file_id"] = file_info['id']
            cleaned_item = {k: v for k, v in item.items() if v not in ["", None]}
            rows.append(cleaned_item)
        return rows
    
    def safe_extract(self, agent, file_path: str, retries: int = 3):
        for attempt in range(1, retries + 1):
            try:
                self.log(f"Extracting data (attempt {attempt}/{retries})...", "INFO")
                result = agent.extract(file_path)
                self.log("Extraction successful", "SUCCESS")
                return result
            except Exception as e:
                self.log(f"Extraction attempt {attempt} failed: {str(e)}", "WARNING")
                time.sleep(2)
        self.log(f"Extraction failed after {retries} attempts", "ERROR")
        return None
    
    def save_failed_extractions(self, spreadsheet_id: str, sheet_name: str, failed_files: List[Dict]):
        """Save failed/incomplete extraction details to a dedicated sheet"""
        try:
            # Clear existing data
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                body={}
            ).execute()
            
            # Add headers
            headers = [[
                'Timestamp',
                'File Name',
                'File ID',
                'Status',
                'Items Extracted',
                'Completeness Score',
                'Issues',
                'Attempts',
                'Strategy Used'
            ]]
            self.append_to_google_sheet(spreadsheet_id, sheet_name, headers)
            
            # Add data rows
            data = []
            for f in failed_files:
                data.append([
                    f.get('timestamp', ''),
                    f.get('file_name', ''),
                    f.get('file_id', ''),
                    f.get('status', ''),
                    f.get('items_extracted', 0),
                    f"{f.get('completeness_score', 0):.2%}",
                    '; '.join(f.get('issues', [])),
                    f.get('attempts', 0),
                    f.get('strategy_used', '')
                ])
            
            if data:
                success = self.append_to_google_sheet(spreadsheet_id, sheet_name, data)
                if success:
                    self.log(f"Saved {len(failed_files)} failed/incomplete extractions to {sheet_name}", "INFO")
                    return True
            
            return False
            
        except Exception as e:
            self.log(f"Failed to save failed extractions: {str(e)}", "ERROR")
            return False
    
    def process_drive_to_sheet_workflow(self, config: dict, skip_existing: bool = True):
        """Process Drive to Sheet workflow for Hyperpure"""
        stats = {
            'total_pdfs': 0,
            'processed_pdfs': 0,
            'failed_pdfs': 0,
            'skipped_pdfs': 0,
            'rows_added': 0,
            'incomplete': 0
        }
        
        if not LLAMA_AVAILABLE:
            self.log("LlamaParse not available", "ERROR")
            return stats
        
        try:
            self.log("Starting Drive to Sheet workflow with LlamaParse", "INFO")
            
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                self.log(f"Could not find agent '{config['llama_agent']}'", "ERROR")
                return stats
            
            self.log("LlamaParse agent found", "SUCCESS")
            
            sheet_name = config['sheet_range'].split('!')[0]
            sheet_id = self.get_sheet_id(config['spreadsheet_id'], sheet_name)
            
            # Get existing IDs first
            existing_ids = set()
            if skip_existing:
                existing_ids = self.get_existing_drive_ids(config['spreadsheet_id'], config['sheet_range'])
                self.log(f"Found {len(existing_ids)} files already in sheet", "INFO")
            
            # Get PDFs from drive
            pdf_files = self.list_drive_pdfs(config['drive_folder_id'], config.get('days_back', 7))
            stats['total_pdfs'] = len(pdf_files)
            
            if skip_existing:
                original_count = len(pdf_files)
                pdf_files = [f for f in pdf_files if f['id'] not in existing_ids]
                stats['skipped_pdfs'] = original_count - len(pdf_files)
                self.log(f"After filtering, {len(pdf_files)} PDFs to process", "INFO")
            
            max_files = config.get('max_files')
            if max_files is not None:
                pdf_files = pdf_files[:max_files]
                self.log(f"Limited to {len(pdf_files)} PDFs after max_files limit", "INFO")
            
            if not pdf_files:
                self.log("No PDF files found to process", "WARNING")
                return stats
            
            self.log(f"Found {len(pdf_files)} PDF files to process", "INFO")
            
            headers = self.get_sheet_headers(config['spreadsheet_id'], sheet_name)
            headers_set = False
            incomplete_extractions = []
            
            for pdf_file in pdf_files:
                try:
                    self.log(f"Processing: {pdf_file['name']}")
                    
                    file_data = self.download_from_drive(pdf_file['id'], pdf_file['name'])
                    if not file_data:
                        self.log(f"Failed to download {pdf_file['name']}", "ERROR")
                        stats['failed_pdfs'] += 1
                        incomplete_extractions.append({
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'file_name': pdf_file['name'],
                            'file_id': pdf_file['id'],
                            'status': 'Download Failed',
                            'items_extracted': 0,
                            'completeness_score': 0,
                            'issues': ['Failed to download from Drive'],
                            'attempts': 0,
                            'strategy_used': 'N/A'
                        })
                        continue
                    
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                        tmp_file.write(file_data)
                        tmp_path = tmp_file.name
                    
                    try:
                        extraction_result = self.safe_extract(agent, tmp_path, retries=5)
                        
                        if not extraction_result:
                            self.log(f"Extraction failed for {pdf_file['name']}", "ERROR")
                            stats['failed_pdfs'] += 1
                            incomplete_extractions.append({
                                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'file_name': pdf_file['name'],
                                'file_id': pdf_file['id'],
                                'status': 'Extraction Failed',
                                'items_extracted': 0,
                                'completeness_score': 0,
                                'issues': ['All extraction attempts failed'],
                                'attempts': 5,
                                'strategy_used': 'standard_retry'
                            })
                            continue
                        
                        # Process extracted data
                        rows_data = self.process_extracted_data(extraction_result.data, pdf_file)
                        
                        if not rows_data:
                            self.log(f"No items found in {pdf_file['name']}", "WARNING")
                            stats['failed_pdfs'] += 1
                            incomplete_extractions.append({
                                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'file_name': pdf_file['name'],
                                'file_id': pdf_file['id'],
                                'status': 'No Rows Extracted',
                                'items_extracted': 0,
                                'completeness_score': 0,
                                'issues': ['No rows after processing'],
                                'attempts': 5,
                                'strategy_used': 'standard_retry'
                            })
                            continue
                        
                        stats['processed_pdfs'] += 1
                        
                        if not headers_set:
                            all_keys = set()
                            for row in rows_data:
                                all_keys.update(row.keys())
                            
                            new_headers = sorted(list(all_keys))
                            
                            if headers:
                                combined = list(dict.fromkeys(headers + new_headers))
                                if combined != headers:
                                    self.update_headers(config['spreadsheet_id'], sheet_name, combined)
                                    headers = combined
                            else:
                                self.update_headers(config['spreadsheet_id'], sheet_name, new_headers)
                                headers = new_headers
                            
                            headers_set = True
                        
                        sheet_rows = []
                        for row_dict in rows_data:
                            row_values = [row_dict.get(h, "") for h in headers]
                            sheet_rows.append(row_values)
                        
                        if self.replace_rows_for_file(
                            spreadsheet_id=config['spreadsheet_id'],
                            sheet_name=sheet_name,
                            file_id=pdf_file['id'],
                            headers=headers,
                            new_rows=sheet_rows,
                            sheet_id=sheet_id
                        ):
                            stats['rows_added'] += len(sheet_rows)
                            self.log(f"Processed {pdf_file['name']}: {len(sheet_rows)} rows added", "SUCCESS")
                        else:
                            stats['failed_pdfs'] += 1
                            self.log(f"Failed to append data for {pdf_file['name']}", "ERROR")
                    
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                
                except Exception as e:
                    self.log(f"Failed to process {pdf_file.get('name', 'unknown')}: {str(e)}", "ERROR")
                    stats['failed_pdfs'] += 1
                    incomplete_extractions.append({
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'file_name': pdf_file.get('name', 'unknown'),
                        'file_id': pdf_file.get('id', ''),
                        'status': 'Processing Error',
                        'items_extracted': 0,
                        'completeness_score': 0,
                        'issues': [str(e)],
                        'attempts': 0,
                        'strategy_used': 'N/A'
                    })
            
            # Save incomplete extractions report
            if incomplete_extractions:
                failed_sheet = config.get('failed_extractions_sheet', 'failed_extractions')
                self.save_failed_extractions(
                    config['spreadsheet_id'],
                    failed_sheet,
                    incomplete_extractions
                )
                stats['incomplete'] = len(incomplete_extractions)
            
            self.log("Drive to Sheet workflow complete!", "INFO")
            self.log(f"PDFs processed: {stats['processed_pdfs']}/{stats['total_pdfs']}", "INFO")
            self.log(f"PDFs skipped: {stats['skipped_pdfs']}", "INFO")
            self.log(f"PDFs failed: {stats['failed_pdfs']}", "INFO")
            self.log(f"Total rows added: {stats['rows_added']}", "INFO")
            self.log(f"Incomplete extractions: {stats['incomplete']}", "INFO")
            
            return stats
            
        except Exception as e:
            self.log(f"Drive to Sheet workflow failed: {str(e)}", "ERROR")
            return stats
    
    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime, 
                             end_time: datetime, stats: dict):
        """Log workflow execution details to Google Sheet"""
        try:
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
            
            if duration >= 60:
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            
            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                duration_str,
                workflow_name,
                stats.get('processed', stats.get('processed_pdfs', 0)),
                stats.get('total_attachments', stats.get('rows_added', 0)),
                stats.get('failed', stats.get('failed_pdfs', 0)),
                stats.get('skipped_pdfs', 0),
                stats.get('incomplete', 0),
                "Success" if stats.get('success', stats.get('processed_pdfs', 0) > 0) else "Failed"
            ]
            
            log_config = CONFIG['workflow_log']
            
            headers = self.get_sheet_headers(log_config['spreadsheet_id'], log_config['sheet_range'])
            if not headers:
                header_row = [
                    "Start Time", "End Time", "Duration", "Workflow", 
                    "Processed", "Total Items", "Failed", "Skipped", "Incomplete", "Status"
                ]
                self.append_to_google_sheet(
                    log_config['spreadsheet_id'], 
                    log_config['sheet_range'], 
                    [header_row]
                )
            
            self.append_to_google_sheet(
                log_config['spreadsheet_id'],
                log_config['sheet_range'],
                [log_row]
            )
            
            self.log(f"Logged workflow: {workflow_name}")
            
        except Exception as e:
            self.log(f"Failed to log workflow: {str(e)}")
    
    def save_remaining_files(self, spreadsheet_id: str, sheet_name: str, files: List[Dict]):
        """Save list of remaining files to the specified sheet"""
        try:
            # Clear existing data in the sheet
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                body={}
            ).execute()
            self.log(f"Cleared existing data in {sheet_name}", "INFO")
            
            # Add headers
            headers = [['File Name', 'File ID', 'Created Time']]
            self.append_to_google_sheet(spreadsheet_id, sheet_name, headers)
            
            # Add data rows
            data = [[f['name'], f['id'], f.get('createdTime', '')] for f in files]
            success = self.append_to_google_sheet(spreadsheet_id, sheet_name, data)
            if success:
                self.log(f"Saved {len(files)} remaining files to {sheet_name}", "SUCCESS")
                return True
            else:
                self.log(f"Failed to save remaining files to {sheet_name}", "ERROR")
                return False
        except Exception as e:
            self.log(f"Failed to save remaining files: {str(e)}", "ERROR")
            return False
    
    def run_scheduled_workflow(self):
        """Run both workflows in sequence, log results, and send email summary"""
        try:
            self.log("=" * 80)
            self.log("STARTING HYPERPURE SCHEDULED WORKFLOW RUN")
            self.log("=" * 80)
            
            overall_start = datetime.now(timezone.utc)
            
            # Workflow 1: Mail to Drive
            self.log("\n[WORKFLOW 1/2] Starting Mail to Drive workflow...")
            mail_start = datetime.now(timezone.utc)
            mail_stats = self.process_mail_to_drive_workflow(CONFIG['mail'])
            mail_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Mail to Drive", mail_start, mail_end, mail_stats)
            
            # Small delay between workflows
            time.sleep(5)
            
            # Workflow 2: Drive to Sheet
            self.log("\n[WORKFLOW 2/2] Starting Drive to Sheet workflow...")
            sheet_start = datetime.now(timezone.utc)
            sheet_stats = self.process_drive_to_sheet_workflow(CONFIG['sheet'], skip_existing=True)
            sheet_end = datetime.now(timezone.utc)
            
            sheet_stats_for_log = {
                'processed_pdfs': sheet_stats['processed_pdfs'],
                'rows_added': sheet_stats['rows_added'],
                'failed_pdfs': sheet_stats['failed_pdfs'],
                'skipped_pdfs': sheet_stats['skipped_pdfs'],
                'incomplete': sheet_stats.get('incomplete', 0),
                'success': sheet_stats['processed_pdfs'] > 0
            }
            self.log_workflow_to_sheet("Drive to Sheet", sheet_start, sheet_end, sheet_stats_for_log)
            
            # Handle remaining files
            drive_files = self.list_drive_pdfs(CONFIG['sheet']['drive_folder_id'], all_time=True)
            existing_ids = self.get_existing_drive_ids(CONFIG['sheet']['spreadsheet_id'], CONFIG['sheet']['sheet_range'])
            
            if len(drive_files) > len(existing_ids):
                remaining_ids = set(f['id'] for f in drive_files) - existing_ids
                remaining_files = [f for f in drive_files if f['id'] in remaining_ids]
                self.save_remaining_files(
                    CONFIG['remaining_files']['spreadsheet_id'],
                    CONFIG['remaining_files']['sheet_range'],
                    remaining_files
                )
            
            overall_end = datetime.now(timezone.utc)
            total_duration = (overall_end - overall_start).total_seconds()
            
            # Format duration for display
            duration_str = f"{total_duration:.2f}s"
            if total_duration >= 60:
                minutes = int(total_duration // 60)
                seconds = int(total_duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            
            # Prepare summary data for email
            summary_data = {
                'days_back': CONFIG['mail']['days_back'],
                'mail_emails_processed': mail_stats.get('emails_processed', 0),
                'mail_attachments_uploaded': mail_stats.get('processed', 0),
                'mail_upload_failed': mail_stats.get('failed', 0),
                'drive_files_found': sheet_stats.get('total_pdfs', 0),
                'drive_files_skipped': sheet_stats.get('skipped_pdfs', 0),
                'drive_files_processed': sheet_stats.get('processed_pdfs', 0),
                'drive_files_failed': sheet_stats.get('failed_pdfs', 0),
                'drive_incomplete_extractions': sheet_stats.get('incomplete', 0),
                'drive_rows_added': sheet_stats.get('rows_added', 0),
                'total_duration': duration_str,
                'overall_success': mail_stats.get('success', False) and sheet_stats.get('processed_pdfs', 0) > 0,
                'any_success': mail_stats.get('success', False) or sheet_stats.get('processed_pdfs', 0) > 0,
                'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Send email notification
            self.log("\n[SENDING EMAIL] Preparing and sending workflow summary...")
            email_sent = self.send_email_notification(summary_data)
            
            if email_sent:
                self.log("[EMAIL] Summary email sent successfully!")
            else:
                self.log("[EMAIL WARNING] Failed to send summary email")
            
            self.log("\n" + "=" * 80)
            self.log("HYPERPURE SCHEDULED WORKFLOW RUN COMPLETED")
            self.log(f"Total Duration: {duration_str}")
            self.log(f"Mail to Drive: {mail_stats.get('processed', 0)} attachments uploaded from {mail_stats.get('emails_processed', 0)} emails")
            self.log(f"Drive to Sheet: {sheet_stats.get('processed_pdfs', 0)} PDFs processed, {sheet_stats.get('rows_added', 0)} rows added")
            self.log("=" * 80 + "\n")
            
            return summary_data
            
        except Exception as e:
            self.log(f"Scheduled workflow failed: {str(e)}", "ERROR")
            return None


def main():
    """Main function to run the scheduler"""
    print("=" * 80)
    print("HYPERPURE GRN SCHEDULER")
    print("Runs every 3 hours: Mail to Drive → Drive to Sheet")
    print("=" * 80)
    
    automation = HyperpureAutomation()
    
    # Authenticate
    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed. Please check credentials.")
        return
    
    print("Authentication successful!")
    
    # Run immediately on start
    print("\nRunning initial workflow...")
    summary = automation.run_scheduled_workflow()
    
    if summary:
        print("\nWorkflow Summary:")
        print(f"  Days Back: {summary['days_back']}")
        print(f"  Mail to Drive: {summary['mail_attachments_uploaded']} attachments uploaded")
        print(f"  Drive to Sheet: {summary['drive_files_processed']} files processed")
        print(f"  Email sent to: {', '.join(CONFIG['notifications']['recipients'])}")
    
    # Schedule to run every 3 hours
    schedule.every(3).hours.do(automation.run_scheduled_workflow)
    
    print(f"\nScheduler started. Next run in 3 hours.")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop the scheduler\n")
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
        print("=" * 80)


if __name__ == "__main__":
    main()
