import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

from chatchef.settings import GOOGLE_CLIENT_SECRET_FILE


class GoogleSheetsService:
    def __init__(self, application_name, spreadsheet_id, sheet_name):
        self.application_name = application_name
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name

    def get_credential(self):
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CLIENT_SECRET_FILE,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        return service

    def get_sheet_data(self):
        range_ = f'{self.sheet_name}!A1:Z'
        service = self.get_credential()
        request = service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range_)
        response = request.execute()
        values = response.get('values', [])

        column_names = [chr(65 + i) for i in range(26)]  # A to Z column names
        rows = []

        for row_index, row in enumerate(values):
            row_dict = {}

            for i, column_name in enumerate(column_names):
                value = row[i] if i < len(row) else ''
                row_dict[f'{column_name}{row_index + 1}'] = value

            rows.append(row_dict)

        json_data = json.dumps(rows, indent=4)
        # Write the JSON to a file or console output

        return rows

    def add_sheet_data(self, col_from, col_to, data):
        service = self.get_credential()

        written_range = f'{self.sheet_name}!A1:Z'

        request = service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=written_range)
        response = request.execute()
        last_row = len(response.get('values', [])) + 1

        for i in range(2):
            if i == 0:
                range_ = f'{self.sheet_name}!{col_from}{last_row}'
                new_row = [data['DishName']]
                value_range = {'values': [new_row]}

                append_request = service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_,
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body=value_range
                )
                append_request.execute()
            elif i == 1:
                previous_alphabet = chr(ord(col_to) - 2)
                range_ = f'{self.sheet_name}!{previous_alphabet}{last_row}:{col_to}{last_row}'
                new_row = [data['ImageV1'], data['ImageV2'], data['ImageV3']]
                value_range = {'values': [new_row]}

                update_request = service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_,
                    valueInputOption='RAW',
                    body=value_range
                )
                update_request.execute()

    def update_sheet_data(self, cell_datas):
        service = self.get_credential()

        for item in cell_datas:
            rng = f"{self.sheet_name}!{item['cellNo']}:{item['cellNo']}"
            new_row = [item['cellSrc']]
            value_range = {
                'values': [new_row]
            }

            append_request = service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                body=value_range,
                valueInputOption='RAW'
            )
            append_request.execute()
