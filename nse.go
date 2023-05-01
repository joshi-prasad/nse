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

	"github.com/golang/glog"
)

const (
	kOcRecords                = "records"
	kOcRecordsExpiryDates     = "expiryDates"
	kOcRecordsTimestamp       = "timestamp"
	kOcRecordsUnderlyingValue = "underlyingValue"
	kOcRecordsStrikePrices    = "strikePrices"
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
	fetchedJson map[string]interface{}
}

func NewNseOcResponse(resp map[string]interface{}) *NseOcResponse {
	return &NseOcResponse{
		fetchedJson: resp,
	}
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

func (self *NseOcResponse) getRecordsStrField(
	records map[string]interface{},
	field string) (string, error) {

	fieldInt, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed. Field %s not found.",
			field)
		glog.Error(msg)
		return "", errors.New(msg)
	}
	value, ok := fieldInt.(string)
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed."+
			"Field %s is not of string type.", field)
		glog.Error(msg)
		return "", errors.New(msg)
	}
	return value, nil
}

func (self *NseOcResponse) getRecordsFloat64Field(
	records map[string]interface{}, field string) (float64, error) {
	fieldValue, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing records failed. Field %s not found.", field)
		glog.Error(msg)
		return 0, errors.New(msg)
	}

	value, ok := fieldValue.(float64)
	if !ok {
		msg := fmt.Sprintf("Parsing records failed."+
			"Field %s is not of float64 type.", field)
		glog.Error(msg)
		return 0, errors.New(msg)
	}

	return value, nil
}

func (self *NseOcResponse) getRecordsArrayField(
	records map[string]interface{},
	field string) ([]interface{}, error) {

	fieldInt, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed. Field %s not found.",
			field)
		return []interface{}{}, errors.New(msg)
	}
	value, ok := fieldInt.([]interface{})
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed."+
			"Field %s is not of array type.", field)
		return []interface{}{}, errors.New(msg)
	}
	return value, nil
}

func (self *NseOcResponse) convertToStringSlice(
	data []interface{}) []string {
	result := make([]string, len(data))
	for i, v := range data {
		if str, ok := v.(string); ok {
			result[i] = str
		} else {
			// Handle the case where the element is not a string
			// You can choose to skip, ignore, or perform some other action
			result[i] = ""
			glog.Info(fmt.Sprintf("Value %v is not string.", v))
		}
	}
	return result
}

func (self *NseOcResponse) convertToIntSlice(
	data []interface{}) []int32 {
	result := make([]int32, len(data))
	for i, v := range data {
		if str, ok := v.(int32); ok {
			result[i] = str
		} else {
			// Handle the case where the element is not a string
			// You can choose to skip, ignore, or perform some other action
			result[i] = 0
			glog.Info(fmt.Sprintf("Value %v is not integer.", v))
		}
	}
	return result
}

func (self *NseOcResponse) ExpiryDates() ([]string, error) {
	records, err := self.parseRecords()
	if err != nil {
		return []string{}, err
	}
	arrayStrInt, err := self.getRecordsArrayField(records, kOcRecordsExpiryDates)
	if err != nil {
		return []string{}, err
	}
	return self.convertToStringSlice(arrayStrInt), nil
}

func (self *NseOcResponse) Timestamp() (string, error) {
	records, err := self.parseRecords()
	if err != nil {
		return "", err
	}
	return self.getRecordsStrField(records, kOcRecordsTimestamp)
}

func (self *NseOcResponse) UnderlyingValue() (float64, error) {
	records, err := self.parseRecords()
	if err != nil {
		return 0, err
	}
	return self.getRecordsFloat64Field(records, kOcRecordsUnderlyingValue)
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
		default:
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
	return NewNseOcResponse(jsonData), nil
}
