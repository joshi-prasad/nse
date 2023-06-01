package nse

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/glog"
)

const (
	kOcBankNifty     = "BANKNIFTY"
	kOcBankNiftyStep = 100
	kOcNifty         = "NIFTY"
	kOcNiftyStep     = 50
	kOcFinNifty      = "FINNIFTY"
	kOcFinNiftyStep  = 50

	kOcFiltered     = "filtered"
	kOcFilteredData = "data"

	kOcRecords                = "records"
	kOcRecordsExpiryDates     = "expiryDates"
	kOcRecordsTimestamp       = "timestamp"
	kOcRecordsUnderlyingValue = "underlyingValue"
	kOcRecordsStrikePrices    = "strikePrices"
	kOcRecordsDataExpiryDate  = "expiryDate"
	kOcRecordsDataStrikePrice = "strikePrice"
	kOcRecordsDataPe          = "PE"
	kOcRecordsDataCe          = "CE"

	kOcRowOpenInterest         = "openInterest"
	kOcRowChangeinOpenInterest = "changeinOpenInterest"
	kOcRowLastPrice            = "lastPrice"
	kOcRowTotalTradedVolume    = "totalTradedVolume"
)

type NseResponse struct {
	respBuf *bytes.Buffer
}

func NewNseResponse(buf *bytes.Buffer) *NseResponse {
	return &NseResponse{
		respBuf: buf,
	}
}

func (self *NseResponse) ResponseBuffer() *bytes.Buffer {
	return self.respBuf
}

type NseOcResponse struct {
	symbol      string
	fetchedJson map[string]interface{}
	step        int32
}

func NewNseOcResponse(
	symbol string,
	resp map[string]interface{}) *NseOcResponse {
	return &NseOcResponse{
		symbol:      symbol,
		fetchedJson: resp,
		step:        0,
	}
}

func (self *NseOcResponse) SetOptionStep(step int32) {
	self.step = step
}

func (self *NseOcResponse) parseRecords() (map[string]interface{}, error) {
	recordInt, ok := self.fetchedJson[kOcRecords]
	if !ok {
		msg := fmt.Sprintf("Parsing OC failed. Field %s not found.", kOcRecords)
		glog.Error(msg)
		return map[string]interface{}{}, errors.New(msg)
	}
	records, ok := recordInt.(map[string]interface{})
	if !ok {
		msg := fmt.Sprintf("Parsing OC failed. Incorrect field %s type.",
			kOcRecords)
		glog.Error(msg)
		return map[string]interface{}{}, errors.New(msg)
	}
	return records, nil
}

func (self *NseOcResponse) ExpiryDates() ([]string, error) {
	records, err := self.parseRecords()
	if err != nil {
		return []string{}, err
	}
	arrayStrInt, err := getArrayField(records, kOcRecordsExpiryDates)
	if err != nil {
		return []string{}, err
	}
	return convertToStringSlice(arrayStrInt), nil
}

func (self *NseOcResponse) Timestamp() (string, error) {
	records, err := self.parseRecords()
	if err != nil {
		return "", err
	}
	return getStrField(records, kOcRecordsTimestamp)
}

func (self *NseOcResponse) UnderlyingValue() (float64, error) {
	records, err := self.parseRecords()
	if err != nil {
		return 0, err
	}
	return getFloat64Field(records, kOcRecordsUnderlyingValue)
}

func (self *NseOcResponse) parseFiltered() (map[string]interface{}, error) {
	recordInt, ok := self.fetchedJson[kOcRecords]
	if !ok {
		msg := fmt.Sprintf("Parsing OC failed. Field %s not found.", kOcRecords)
		glog.Error(msg)
		return map[string]interface{}{}, errors.New(msg)
	}
	records, ok := recordInt.(map[string]interface{})
	if !ok {
		msg := fmt.Sprintf("Parsing OC failed. Incorrect field %s type.",
			kOcRecords)
		glog.Error(msg)
		return map[string]interface{}{}, errors.New(msg)
	}
	return records, nil
}

func (self *NseOcResponse) FilteredData() ([]interface{}, error) {
	records, err := self.parseFiltered()
	if err != nil {
		return []interface{}{}, err
	}
	return getArrayField(records, kOcFilteredData)
}

func (self *NseOcResponse) stringExists(arr []string, target string) bool {
	for _, str := range arr {
		if str == target {
			return true
		}
	}
	return false
}

func (self *NseOcResponse) GetExpiryOc(
	symbol string,
	expiryDate string) (*NseOc, error) {

	availableExpiries, err := self.ExpiryDates()
	if err != nil {
		glog.Error("Failed to fetch available expiry dates for option chain.")
		return nil, err
	}
	if !self.stringExists(availableExpiries, expiryDate) {
		msg := fmt.Sprintf("No option chain for expiry=%s.", expiryDate)
		glog.Error(msg)
		glog.Error(availableExpiries)
		return nil, errors.New(msg)
	}

	timestamp, err := self.Timestamp()
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch timestamp.")
		glog.Error(msg)
		return nil, errors.New(msg)
	}

	underlyingValue, err := self.UnderlyingValue()
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch underlying asset value.")
		glog.Error(msg)
		return nil, errors.New(msg)
	}

	recordsDataInt, err := self.FilteredData()
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch option chain data records.")
		glog.Error(msg)
		return nil, errors.New(msg)
	}

	expiryDataRecords, err :=
		self.getExpiryDataRecords(expiryDate, recordsDataInt)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch data records with error=%s", err)
		glog.Error(msg)
		return nil, err
	}

	oc := NewNseOc(symbol, expiryDate, timestamp, underlyingValue)
	oc.SetOcDataRecords(expiryDataRecords)
	return oc, nil
}

func (self *NseOcResponse) getExpiryDataRecords(
	expiryDate string,
	recordsDataInt []interface{}) ([]map[string]interface{}, error) {

	expiryDataRecords := []map[string]interface{}{}
	for _, recordInt := range recordsDataInt {
		dataRecord, ok := recordInt.(map[string]interface{})
		if !ok {
			msg := fmt.Sprintf("Unexpected data record.")
			return []map[string]interface{}{}, errors.New(msg)
		}
		dataRecordExpiryDate, err :=
			getStrField(dataRecord, kOcRecordsDataExpiryDate)
		if err != nil {
			msg := fmt.Sprintf("Failed to fetch expiry data from data records.")
			return []map[string]interface{}{}, errors.New(msg)
		}
		if dataRecordExpiryDate != expiryDate {
			continue
		}
		expiryDataRecords = append(expiryDataRecords, dataRecord)
	}
	return expiryDataRecords, nil
}

type NSE struct {
	fetchCookie              bool
	cookie                   *http.Cookie
	urlOc                    string
	urlIndex                 string
	fnoParticipantOiUrlPreix string
	session                  *http.Client
	cookies                  map[string]string
	headers                  map[string]string
}

func NewNSE() *NSE {
	return &NSE{
		fetchCookie:              true,
		cookie:                   nil,
		urlOc:                    "https://www.nseindia.com/option-chain",
		urlIndex:                 "https://www.nseindia.com/api/option-chain-indices?symbol=",
		fnoParticipantOiUrlPreix: "https://archives.nseindia.com/content/nsccl/fao_participant_oi_",
		session:                  &http.Client{},
		cookies:                  make(map[string]string),
		headers: map[string]string{
			"user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
			"accept-language": "en,gu;q=0.9,hi;q=0.8",
			"accept-encoding": "gzip",
		},
	}
}

func (self *NSE) FetchCookie() {
	urlStr := self.urlOc
	req, _ := http.NewRequest("GET", urlStr, nil)
	for k, v := range self.headers {
		req.Header.Set(k, v)
	}

	glog.Info("Fetching URL ", urlStr)
	resp, err := self.session.Do(req)
	if err != nil {
		msg := fmt.Sprintf("Fetching %s failed with error %s\n", urlStr, err)
		glog.Error(msg)
		self.fetchCookie = true
		return
	}

	for _, c := range resp.Cookies() {
		self.cookies[c.Name] = c.Value
	}
}

func (self *NSE) NewGetRequest(url string) *http.Request {
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range self.headers {
		req.Header.Set(k, v)
	}
	for k, v := range self.cookies {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}
	return req
}

func (self *NSE) FetchUrl(url string) (
	*http.Response, *NseResponse, error) {

	var resp *http.Response = nil
	var err error

	retry := true
	retryCount := 0
	for retry {
		if self.fetchCookie {
			self.FetchCookie()
		}
		self.fetchCookie = false

		glog.Info("Fetching URL ", url)
		req := self.NewGetRequest(url)

		resp, err = self.session.Do(req)
		if err != nil {
			msg := fmt.Sprintf("Fetching URL=%s failed with error=%s", url, err)
			glog.Error(msg)
			return nil, nil, err
		}

		retry = true
		errMsg := fmt.Sprintf("Fetching URL=%s failed with status=%d. ",
			url, resp.StatusCode)
		switch resp.StatusCode {
		case http.StatusOK:
			retry = false
			break
		case http.StatusUnauthorized:
			// http.StatusUnauthorized is 401
			glog.Error(errMsg, "Fetching Cookie.")
			self.fetchCookie = true
		case http.StatusForbidden:
			// 403
			glog.Error(errMsg, "Sleeping for 5 minutes.")
			self.fetchCookie = true
			time.Sleep(5 * time.Minute)
		default:
			retryCount += 1
			if retryCount >= 5 {
				return nil, nil, errors.New("Failed with error " + strconv.Itoa(resp.StatusCode))
			}
			time.Sleep(1 * time.Second)
			glog.Error(errMsg, "Retrying...")
		}
	}

	var respBuf *bytes.Buffer
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		respBuf, err = self.readGzipResponse(resp)
	default:
		respBuf, err = self.readResponse(resp)
	}
	if err != nil {
		msg := fmt.Sprintf("Reading the HTTP response failed with error=%s", err)
		glog.Error(msg)
		return nil, nil, err
	}

	msg := fmt.Sprintf("Successfully fetched URL=%s.", url)
	glog.Info(msg)
	return resp, NewNseResponse(respBuf), nil
}

func (self *NSE) readGzipResponse(
	resp *http.Response) (*bytes.Buffer, error) {

	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (self *NSE) readResponse(resp *http.Response) (*bytes.Buffer, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(body), err
}

func (self *NSE) FetchOptionChainUrl(symbol string) (*NseOcResponse, error) {
	ocUrl := self.urlIndex + url.PathEscape(symbol)
	_, resp, err := self.FetchUrl(ocUrl)
	if err != nil {
		msg := fmt.Sprintf("Fetching OC for symbol=%s failed with err=%s",
			symbol, err)
		glog.Error(msg)
		return nil, err
	}

	var jsonData map[string]interface{}
	err = json.Unmarshal(resp.ResponseBuffer().Bytes(), &jsonData)
	if err != nil {
		msg := fmt.Sprintf("Parsing OC response failed with error=%s.", err)
		glog.Error(msg)
		return nil, err
	}
	return NewNseOcResponse(symbol, jsonData), nil
}

func (self *NSE) FetchOptionChain(symbol string, expiryDate string) (*NseOc, error) {
	fetchResp, err := self.FetchOptionChainUrl(symbol)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch %s option chain.", symbol)
		glog.Error(msg)
		return nil, err
	}
	return fetchResp.GetExpiryOc(symbol, expiryDate)
}

func (self *NSE) FetchBankNiftyOc(expiryDate string) (*NseOc, error) {
	oc, err := self.FetchOptionChain(kOcBankNifty, expiryDate)
	if err != nil {
		return nil, err
	}
	oc.SetStrikeStep(kOcBankNiftyStep)
	return oc, nil
}

func (self *NSE) FetchNiftyOc(expiryDate string) (*NseOc, error) {
	oc, err := self.FetchOptionChain(kOcNifty, expiryDate)
	if err != nil {
		return nil, err
	}
	oc.SetStrikeStep(kOcNiftyStep)
	return oc, nil
}

func (self *NSE) FetchFinNiftyOc(expiryDate string) (*NseOc, error) {
	oc, err := self.FetchOptionChain(kOcFinNifty, expiryDate)
	if err != nil {
		return nil, err
	}
	oc.SetStrikeStep(kOcFinNiftyStep)
	return oc, nil
}

func (self *NSE) FetchFOParticipantData(
	date time.Time) ([]NseFODataRecord, error) {

	suffix := NseFOData{}.DateToNseFOtData(date)
	url := fmt.Sprintf("%s%s.csv", self.fnoParticipantOiUrlPreix, suffix)
	_, data, err := self.FetchUrl(url)
	if err != nil {
		msg := fmt.Sprintf("Fetching futures data failed with error=%s", err)
		glog.Error(msg)
		return nil, err
	}

	return NseFOData{}.Parse(data.ResponseBuffer())
}
