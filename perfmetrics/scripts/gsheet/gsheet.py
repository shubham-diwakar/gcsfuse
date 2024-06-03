import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1kvHv1OBCzr9GnFxRu9RTJC7jjQjc9M4rAiDnhyak2Sg'
SPREADSHEET_ID_96_MACHINE = '14-hU4_PWqY6ETSlW-uJPww9psw_usasYbnTTxO7brpc'

CREDENTIALS_PATH = ('./gsheet/creds.json')

def _get_sheets_service_client():
  creds = service_account.Credentials.from_service_account_file(
      CREDENTIALS_PATH, scopes=SCOPES)
  service = build('sheets', 'v4', credentials=creds)
  return service


def write_to_google_sheet(worksheet: str, data) -> None:
  spreadsheet_id = SPREADSHEET_ID
  if os.environ.get("MACHINE_TYPE") == "n2-standard-96" :
    spreadsheet_id = SPREADSHEET_ID_96_MACHINE
    print("In n2-standard-96 machine")
  else :
    os.exit(1)

  """Calls the API to update the values of a sheet.

  Args:
    worksheet: string, name of the worksheet to be edited appended by a "!"
    data: list of tuples/lists, data to be added to the worksheet

  Raises:
    HttpError: For any Google Sheets API call related errors
  """
  sheets_client = _get_sheets_service_client()

  # Getting the index of the last occupied row in the sheet
  spreadsheet_response = sheets_client.spreadsheets().values().get(
      spreadsheetId=spreadsheet_id,
      range='{}!A1:A'.format(worksheet)).execute()
  entries = len(spreadsheet_response['values'])

  # Clearing the occupied rows
  request = sheets_client.spreadsheets().values().clear(
      spreadsheetId=spreadsheet_id,
      range='{}!A2:{}'.format(worksheet,entries+1), 
      body={}).execute()

  # Appending new rows
  sheets_client.spreadsheets().values().update(
      spreadsheetId=spreadsheet_id,
      valueInputOption='USER_ENTERED',
      body={
          'majorDimension': 'ROWS',
          'values': data
      },
      range='{}!A2'.format(worksheet)).execute()
