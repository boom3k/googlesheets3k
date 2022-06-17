package googlesheets3k

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"log"
	"net/http"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/sheets/v4"
)

type GoogleSheets3k struct {
	Service *sheets.Service
	Subject string
}

func BuildAPI(client *http.Client, subject string, ctx context.Context) *GoogleSheets3k {
	newDriveAPI := &GoogleSheets3k{}
	service, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf(err.Error())
	}
	newDriveAPI.Service = service
	newDriveAPI.Subject = subject
	return newDriveAPI
}

func BuildGoogleSheets3kOAuth2(subject string, scopes []string, clientSecret, authorizationToken []byte, ctx context.Context) *GoogleSheets3k {
	config, err := google.ConfigFromJSON(clientSecret, scopes...)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	token := &oauth2.Token{}
	err = json.Unmarshal(authorizationToken, token)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	client := config.Client(context.Background(), token)
	return BuildAPI(client, subject, ctx)
}

func BuildGoogleSheets3kImpersonation(subject string, scopes []string, serviceAccountKey []byte, ctx context.Context) *GoogleSheets3k {
	jwt, err := google.JWTConfigFromJSON(serviceAccountKey, scopes...)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	jwt.Subject = subject
	return BuildAPI(jwt.Client(ctx), subject, ctx)
}

func (receiver *GoogleSheets3k) PrintToSheet(spreadsheetId, a1Notation, majorDimension string, values [][]interface{}, overwrite bool) interface{} {
	var valueRange sheets.ValueRange
	valueRange.MajorDimension = strings.ToUpper(majorDimension)
	valueRange.Values = values
	log.Println("Spreadsheet Write Request --> SpreadsheetID:[" + spreadsheetId + "], A1Notation:[" + a1Notation + "], TotalInserts[" + fmt.Sprint(len(values)) + "], overwrite[" + fmt.Sprint(overwrite) + "]")
	if overwrite == true {
		response, err := receiver.Service.Spreadsheets.Values.Update(spreadsheetId, a1Notation, &valueRange).ValueInputOption("RAW").Do()
		if err != nil {
			log.Fatalf(err.Error())
		}
		return response
	}
	response, err := receiver.Service.Spreadsheets.Values.Append(spreadsheetId, a1Notation, &valueRange).ValueInputOption("USER_ENTERED").Fields("*").Do()
	if err != nil {
		log.Println(err.Error())
		if strings.Contains(err.Error(), "Quota exceeded") {
			log.Println("Backing off for 2.5 seconds...")
			time.Sleep(time.Millisecond * 2500)
			return receiver.PrintToSheet(spreadsheetId, a1Notation, majorDimension, values, overwrite)
		}
		log.Fatal(err.Error())
	}
	log.Println("Spreadsheet write request was successful...")
	return response
}

func (receiver *GoogleSheets3k) CreateSpreadsheet(spreadtabName string) *sheets.Spreadsheet {
	ss := &sheets.Spreadsheet{}
	ss.Properties = &sheets.SpreadsheetProperties{Title: spreadtabName}
	response, err := receiver.Service.Spreadsheets.Create(ss).Fields("*").Do()
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Println("Created spreadsheet -> ", spreadtabName, " [", response.SpreadsheetId, "] @ "+response.SpreadsheetUrl)
	return response
}

func (receiver *GoogleSheets3k) CreateAndPrintToSheet(spreadsheetName, tabName string, values [][]interface{}) {
	log.Printf("Creating spreadsheet: %s\n", spreadsheetName)
	spreadsheet := receiver.CreateSpreadsheet(spreadsheetName)
	receiver.RenameTab(*spreadsheet, spreadsheet.Sheets[0].Properties.Title, tabName)
	receiver.PrintToSheet(spreadsheet.SpreadsheetId, tabName, "ROWS", values, false)
}

func (receiver *GoogleSheets3k) RenameSpreadSheet(spreadsheetId, newTitle string) (*sheets.Spreadsheet, error) {

	spreadsheetProperties := &sheets.SpreadsheetProperties{Title: newTitle}
	updateSpreadsheetPropertiesRequest := &sheets.UpdateSpreadsheetPropertiesRequest{Properties: spreadsheetProperties, Fields: "*"}
	request := &sheets.Request{UpdateSpreadsheetProperties: updateSpreadsheetPropertiesRequest}
	var requests = []*sheets.Request{request}
	batchUpdateSpreadsheetRequest := &sheets.BatchUpdateSpreadsheetRequest{Requests: requests}
	response, err := receiver.Service.Spreadsheets.BatchUpdate(spreadsheetId, batchUpdateSpreadsheetRequest).Fields("*").Do()
	if err != nil {
		log.Println(err, err.Error())
	}
	log.Printf("Renamed SpreadsheetID: [%s] is now \"%s\"\n", spreadsheetId, newTitle)
	return response.UpdatedSpreadsheet, err
}

func (receiver *GoogleSheets3k) InsertTab(spreadsheetId, newTabName string) *sheets.BatchUpdateSpreadsheetResponse {
	properties := &sheets.SheetProperties{Title: newTabName}
	addSheetsRequest := &sheets.AddSheetRequest{Properties: properties}
	request := []*sheets.Request{{AddSheet: addSheetsRequest}}
	content := &sheets.BatchUpdateSpreadsheetRequest{Requests: request}
	response, err := receiver.Service.Spreadsheets.BatchUpdate(spreadsheetId, content).Fields("*").Do()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return response
}

func (receiver *GoogleSheets3k) RenameTabById(spreadsheetId, newTabName string, tabID int64) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	sheetProperties := &sheets.SheetProperties{Title: newTabName, SheetId: tabID}
	updateSheetPropertiesRequest := &sheets.UpdateSheetPropertiesRequest{Properties: sheetProperties, Fields: "title"}
	requests := []*sheets.Request{{UpdateSheetProperties: updateSheetPropertiesRequest}}
	return receiver.ExecuteBatchUpdateRequest(spreadsheetId, requests)
}

func (receiver *GoogleSheets3k) RenameTab(spreadsheet sheets.Spreadsheet, oldTabName, newTabName string) {
	tab := receiver.GetByTabName(spreadsheet, oldTabName)
	receiver.RenameTabById(spreadsheet.SpreadsheetId, newTabName, tab.Properties.SheetId)
}

func (receiver *GoogleSheets3k) DeleteTabById(spreadsheetId string, tabId int64) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	requests := []*sheets.Request{{DeleteSheet: &sheets.DeleteSheetRequest{SheetId: tabId}}}
	return receiver.ExecuteBatchUpdateRequest(spreadsheetId, requests)
}

func (receiver *GoogleSheets3k) DeleteTabByName(spreadsheet sheets.Spreadsheet, tabName string) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	tab := receiver.GetByTabName(spreadsheet, tabName)
	requests := []*sheets.Request{{DeleteSheet: &sheets.DeleteSheetRequest{SheetId: tab.Properties.SheetId}}}
	return receiver.ExecuteBatchUpdateRequest(spreadsheet.SpreadsheetId, requests)
}

func (receiver *GoogleSheets3k) ExecuteBatchUpdateRequest(spreadsheetId string, requests []*sheets.Request) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	content := &sheets.BatchUpdateSpreadsheetRequest{Requests: requests}
	return receiver.Service.Spreadsheets.BatchUpdate(spreadsheetId, content).Fields("*").Do()
}

func (receiver *GoogleSheets3k) GetSheetValues(spreadsheetId, a1Notation string) [][]interface{} {
	sheetOutputValues, err := receiver.Service.Spreadsheets.Values.Get(spreadsheetId, a1Notation).Do()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return sheetOutputValues.Values
}

func (receiver *GoogleSheets3k) GetColumnValues(spreadsheetId, a1Notation string) []interface{} {
	sheetOutputValues, err := receiver.Service.Spreadsheets.Values.Get(spreadsheetId, a1Notation).Do()
	if err != nil {
		log.Fatalf(err.Error())
	}
	var columnValues []interface{}

	for _, row := range sheetOutputValues.Values {
		for i := range row {
			columnValues = append(columnValues, row[i])
		}
	}

	return columnValues
}

// GetSheetValuesMapped Returns Values with a given primary column as a map
func (receiver *GoogleSheets3k) GetSheetValuesMapped(spreadsheetId, a1Notation string, keyColumn int) map[interface{}][][]interface{} {
	m := make(map[interface{}][][]interface{})
	for _, row := range receiver.GetSheetValues(spreadsheetId, a1Notation) {
		var rowCells []interface{}
		for i, cell := range row {
			if i == keyColumn {
				continue
			}
			rowCells = append(rowCells, cell)
		}
		m[row[keyColumn]] = append(m[row[keyColumn]], rowCells)
	}
	return m
}

func (receiver *GoogleSheets3k) GetColumnValuesAsString(spreadsheetId, a1Notation string, toLower bool) []string {
	sheetOutputValues, err := receiver.Service.Spreadsheets.Values.Get(spreadsheetId, a1Notation).Do()
	if err != nil {
		log.Fatalf(err.Error())
	}
	var columnValues []string

	for _, row := range sheetOutputValues.Values {
		for i := range row {
			columnValues = append(columnValues, row[i].(string))
		}
	}

	if toLower {
		for i := range columnValues {
			columnValues[i] = strings.ToLower(columnValues[i])
		}
	}
	return columnValues
}

func (receiver *GoogleSheets3k) GetByTabName(spreadsheet sheets.Spreadsheet, tabName string) *sheets.Sheet {
	for _, sheet := range spreadsheet.Sheets {
		if sheet.Properties.Title == tabName {
			return sheet
		}
	}
	log.Println(googleapi.Error{Body: "Sheet SendEmail " + tabName + " not found in SpreadsheetID: " + spreadsheet.SpreadsheetId, Message: "Sheet not found"})
	return nil
}

func (receiver *GoogleSheets3k) ClearValues(spreadsheetID, a1Notation string) *sheets.ClearValuesResponse {
	response, err := receiver.Service.Spreadsheets.Values.Clear(spreadsheetID, a1Notation, &sheets.ClearValuesRequest{}).Fields("*").Do()
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	log.Printf("Cleared %s [%s]\n", spreadsheetID, a1Notation)
	return response
}
