package nse

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/golang/glog"
)

type NseFODataRecord struct {
	ClientType           string `csv:"Client Type"`
	FutureIndexLong      int    `csv:"Future Index Long"`
	FutureIndexShort     int    `csv:"Future Index Short"`
	FutureStockLong      int    `csv:"Future Stock Long"`
	FutureStockShort     int    `csv:"Future Stock Short"`
	OptionIndexCallLong  int    `csv:"Option Index Call Long"`
	OptionIndexPutLong   int    `csv:"Option Index Put Long"`
	OptionIndexCallShort int    `csv:"Option Index Call Short"`
	OptionIndexPutShort  int    `csv:"Option Index Put Short"`
	OptionStockCallLong  int    `csv:"Option Stock Call Long"`
	OptionStockPutLong   int    `csv:"Option Stock Put Long"`
	OptionStockCallShort int    `csv:"Option Stock Call Short"`
	OptionStockPutShort  int    `csv:"Option Stock Put Short"`
	TotalLongContracts   int    `csv:"Total Long Contracts"`
	TotalShortContracts  int    `csv:"Total Short Contracts"`
}

func (self *NseFODataRecord) NetFutureIndexPosition() int {
	return self.FutureIndexLong - self.FutureIndexShort
}

func (self *NseFODataRecord) NetOptionIndexCallPosition() int {
	return self.OptionIndexCallLong - self.OptionIndexCallShort
}

func (self *NseFODataRecord) NetOptionIndexPutPosition() int {
	return self.OptionIndexPutLong - self.OptionIndexPutShort
}

func (self *NseFODataRecord) NetOptionIndexOpenInterest() int {
	return self.NetOptionIndexCallPosition() - self.NetOptionIndexPutPosition()
}

type NseFOData struct {
}

func (NseFOData) Parse(buffer *bytes.Buffer) ([]NseFODataRecord, error) {
	// Split into lines
	lines := bytes.Split(buffer.Bytes(), []byte{'\n'})

	// Check if first line starts with a double quote and remove it if necessary
	if len(lines) > 0 && lines[0][0] == '"' {
		lines = lines[1:]
	}

	glog.Info("Response ", string(bytes.Join(lines, []byte{'\n'})))

	// Create a CSV reader
	reader := csv.NewReader(bytes.NewReader(bytes.Join(lines, []byte{'\n'})))

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	glog.Info("Header ", header)

	// Create a map to store the indices of the columns in the CSV data
	indices := make(map[string]int)
	for i, col := range header {
		indices[col] = i
	}
	glog.Info("Indices ", indices)

	// Parse the records
	records := []NseFODataRecord{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		record := NseFODataRecord{}
		record.ClientType = row[indices["Client Type"]]
		record.FutureIndexLong, _ = strconv.Atoi(row[indices["Future Index Long"]])
		record.FutureIndexShort, _ = strconv.Atoi(row[indices["Future Index Short"]])
		record.FutureStockLong, _ = strconv.Atoi(row[indices["Future Stock Long"]])
		record.FutureStockShort, _ = strconv.Atoi(row[indices["Future Stock Short"]])
		record.OptionIndexCallLong, _ = strconv.Atoi(row[indices["Option Index Call Long"]])
		record.OptionIndexPutLong, _ = strconv.Atoi(row[indices["Option Index Put Long"]])
		record.OptionIndexCallShort, _ = strconv.Atoi(row[indices["Option Index Call Short"]])
		record.OptionIndexPutShort, _ = strconv.Atoi(row[indices["Option Index Put Short"]])
		record.OptionStockCallLong, _ = strconv.Atoi(row[indices["Option Stock Call Long"]])
		record.OptionStockPutLong, _ = strconv.Atoi(row[indices["Option Stock Put Long"]])
		record.OptionStockCallShort, _ = strconv.Atoi(row[indices["Option Stock Call Short"]])
		record.OptionStockPutShort, _ = strconv.Atoi(row[indices["Option Stock Put Short"]])
		record.TotalLongContracts, _ = strconv.Atoi(row[indices["Total Long Contracts"]])
		record.TotalShortContracts, _ = strconv.Atoi(row[indices["Total Short Contracts"]])

		// glog.Error("Failed to parse ", err)
		records = append(records, record)
	}

	return records, nil
}

func (NseFOData) DateToNseFOtData(date time.Time) string {
	day := date.Day()
	month := date.Month()
	year := date.Year()
	return fmt.Sprintf("%02d%02d%d", day, month, year)
}

func (NseFOData) FileName(date time.Time) string {
	dateStr := NseFOData{}.DateToNseFOtData(date)
	return fmt.Sprintf("fao_participants_oi_%s", dateStr)
}

type NseFuturesRecord struct {
	TotalLong  int
	TotalShort int
	Net        int
	NetChange  int
}

func (self *NseFuturesRecord) Fill(
	today *NseFODataRecord,
	yesterday *NseFuturesRecord) {

	self.TotalLong = today.FutureIndexLong
	self.TotalShort = today.FutureIndexShort
	self.Net = self.TotalLong - self.TotalShort
	if yesterday != nil {
		self.NetChange = self.Net - yesterday.Net
	}
}

type NseOptionsRecord struct {
	TotalCallLong  int
	TotalCallShort int
	TotalPutLong   int
	TotalPutShort  int
	NetCall        int
	NetPut         int
	Pcr            float64
	Net            int

	NetCallChange int
	NetPutChange  int
	NetChange     int
}

func (self *NseOptionsRecord) Fill(
	today *NseFODataRecord,
	yesterday *NseOptionsRecord) {

	self.TotalCallLong = today.OptionIndexCallLong
	self.TotalCallShort = today.OptionIndexCallShort
	self.TotalPutLong = today.OptionIndexPutLong
	self.TotalPutShort = today.OptionIndexPutShort
	self.FillNetValues(yesterday)
}

func (self *NseOptionsRecord) FillNetValues(yesterday *NseOptionsRecord) {

	self.NetCall = self.TotalCallLong - self.TotalCallShort
	self.NetPut = self.TotalPutLong - self.TotalPutShort
	self.Net = self.NetCall - self.NetPut
	self.Pcr = float64(self.NetPut) / float64(self.NetCall)
	if yesterday != nil {
		self.NetCallChange = self.NetCall - yesterday.NetCall
		self.NetPutChange = self.NetPut - yesterday.NetPut
		self.NetChange = self.Net - yesterday.Net
	}
}

type SgxFuturesRecord struct {
	TotalOi       int
	TotalOiChange int
	MaxOi         int
}

type SgxOptionsRecord struct {
	TotalCall int
	TotalPut  int
	Net       int
	NetChange int
	Pcr       float64

	MaxCallOi int
	MaxPutOi  int
}

type NseFOStatsRecord struct {
	Date time.Time

	// Futures Information
	FuturesDii   NseFuturesRecord
	FuturesFii   NseFuturesRecord
	FuturesPro   NseFuturesRecord
	FuturesTotal NseFuturesRecord

	// SGX Nifty Futures
	FuturesSgxNifty SgxFuturesRecord
	FuturesSgxBank  SgxFuturesRecord

	// Options Information
	OptionsFii   NseOptionsRecord
	OptionsPro   NseOptionsRecord
	OptionsTotal NseOptionsRecord

	// SGX Nifty Options
	OptionsSgxNifty SgxOptionsRecord
}

type NseFOStats struct {
	filePath string
	records  []NseFOStatsRecord
}

func NewNseFOStats(fileName string) *NseFOStats {
	return &NseFOStats{
		filePath: fileName,
		records:  []NseFOStatsRecord{},
	}
}

func (self *NseFOStats) ReadFile() error {
	file, err := os.Open(self.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	// Read the header row
	_, err = reader.Read()
	if err != nil {
		return err
	}

	// Read the remaining rows
	var records []NseFOStatsRecord
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		record := NseFOStatsRecord{
			Date:            time.Time{},
			FuturesDii:      NseFuturesRecord{},
			FuturesFii:      NseFuturesRecord{},
			FuturesPro:      NseFuturesRecord{},
			FuturesTotal:    NseFuturesRecord{},
			FuturesSgxNifty: SgxFuturesRecord{},
			FuturesSgxBank:  SgxFuturesRecord{},
			OptionsFii:      NseOptionsRecord{},
			OptionsPro:      NseOptionsRecord{},
			OptionsTotal:    NseOptionsRecord{},
			OptionsSgxNifty: SgxOptionsRecord{},
		}
		record.Date, _ = time.Parse("2006-01-02", row[0])

		record.FuturesDii.TotalLong, _ = strconv.Atoi(row[1])
		record.FuturesDii.TotalShort, _ = strconv.Atoi(row[2])
		record.FuturesDii.Net, _ = strconv.Atoi(row[3])
		record.FuturesDii.NetChange, _ = strconv.Atoi(row[4])

		record.FuturesFii.TotalLong, _ = strconv.Atoi(row[5])
		record.FuturesFii.TotalShort, _ = strconv.Atoi(row[6])
		record.FuturesFii.Net, _ = strconv.Atoi(row[7])
		record.FuturesFii.NetChange, _ = strconv.Atoi(row[8])

		record.FuturesPro.TotalLong, _ = strconv.Atoi(row[9])
		record.FuturesPro.TotalShort, _ = strconv.Atoi(row[10])
		record.FuturesPro.Net, _ = strconv.Atoi(row[11])
		record.FuturesPro.NetChange, _ = strconv.Atoi(row[12])

		record.FuturesTotal.TotalLong, _ = strconv.Atoi(row[13])
		record.FuturesTotal.TotalShort, _ = strconv.Atoi(row[14])
		record.FuturesTotal.Net, _ = strconv.Atoi(row[15])
		record.FuturesTotal.NetChange, _ = strconv.Atoi(row[16])

		record.FuturesSgxNifty.TotalOi, _ = strconv.Atoi(row[17])
		record.FuturesSgxNifty.TotalOiChange, _ = strconv.Atoi(row[17])
		record.FuturesSgxNifty.MaxOi, _ = strconv.Atoi(row[18])

		record.FuturesSgxBank.TotalOi, _ = strconv.Atoi(row[19])
		record.FuturesSgxBank.TotalOiChange, _ = strconv.Atoi(row[20])
		record.FuturesSgxBank.MaxOi, _ = strconv.Atoi(row[21])

		record.OptionsFii.NetCall, _ = strconv.Atoi(row[22])
		record.OptionsFii.NetPut, _ = strconv.Atoi(row[23])
		record.OptionsFii.Net, _ = strconv.Atoi(row[23])
		record.OptionsFii.NetChange, _ = strconv.Atoi(row[24])

		record.OptionsPro.NetCall, _ = strconv.Atoi(row[25])
		record.OptionsPro.NetPut, _ = strconv.Atoi(row[26])
		record.OptionsPro.Net, _ = strconv.Atoi(row[27])
		record.OptionsPro.NetChange, _ = strconv.Atoi(row[28])

		record.OptionsTotal.NetCall, _ = strconv.Atoi(row[29])
		record.OptionsTotal.NetPut, _ = strconv.Atoi(row[30])
		record.OptionsTotal.Net, _ = strconv.Atoi(row[31])
		record.OptionsTotal.Pcr, _ = strconv.ParseFloat(row[32], 64)
		record.OptionsTotal.NetChange, _ = strconv.Atoi(row[33])

		record.OptionsSgxNifty.TotalCall, _ = strconv.Atoi(row[34])
		record.OptionsSgxNifty.TotalPut, _ = strconv.Atoi(row[35])
		record.OptionsSgxNifty.Net, _ = strconv.Atoi(row[36])
		record.OptionsSgxNifty.Pcr, _ = strconv.ParseFloat(row[37], 64)
		record.OptionsSgxNifty.NetChange, _ = strconv.Atoi(row[38])
		record.OptionsSgxNifty.MaxCallOi, _ = strconv.Atoi(row[39])
		record.OptionsSgxNifty.MaxPutOi, _ = strconv.Atoi(row[40])

		records = append(records, record)
	}

	self.records = records
	return nil
}

func (self *NseFOStats) AppendToFile(record *NseFOStatsRecord) error {
	file, err := os.OpenFile(self.filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Check if file is empty, and write header if required
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		if err = writer.Write([]string{
			"Date",

			"IndexFuturesDiiLong",
			"IndexFuturesDiiShort",
			"IndexFuturesDiiNet",
			"IndexFuturesDiiNetChange",

			"IndexFuturesFiiLong",
			"IndexFuturesFiiShort",
			"IndexFuturesFiiNet",
			"IndexFuturesFiiNetChange",

			"IndexFuturesProLong",
			"IndexFuturesProShort",
			"IndexFuturesProNet",
			"IndexFuturesProNetChange",

			"IndexFuturesTotalLong",
			"IndexFuturesTotalShort",
			"IndexFuturesTotalNet",
			"IndexFuturesTotalNetChange",

			"SgxNiftyFuturesOi",
			"SgxNiftyFuturesOiChange",
			"SgxNiftyFuturesMaxOi",

			"SgxBankNiftyFuturesOi",
			"SgxBankNiftyFuturesOiChange",
			"SgxBankNiftyFuturesMaxOi",

			"IndexOptionsFiiCall",
			"IndexOptionsFiiPut",
			"IndexOptionsFiiNet",
			"IndexOptionsFiiNetChange",

			"IndexOptionsProCall",
			"IndexOptionsProPut",
			"IndexOptionsProNet",
			"IndexOptionsProNetChange",

			"IndexOptionsTotalCall",
			"IndexOptionsTotalPut",
			"IndexOptionsTotalNet",
			"IndexOptionsTotalPcr",
			"IndexOptionsTotalNetChange",

			"SgxNiftyOptionsTotalCallOi",
			"SgxNiftyOptionsTotalPutOi",
			"SgxNiftyOptionsNet",
			"SgxNiftyOptionsPcr",
			"SgxNiftyOptionsNetChange",
			"SgxNiftyOptionsMaxCallOi",
			"SgxNiftyOptionMaxPutOi",
		}); err != nil {
			return err
		}
	}

	err = writer.Write([]string{
		record.Date.Format("2006-01-02"),

		strconv.Itoa(record.FuturesDii.TotalLong),
		strconv.Itoa(record.FuturesDii.TotalShort),
		strconv.Itoa(record.FuturesDii.Net),
		strconv.Itoa(record.FuturesDii.NetChange),

		strconv.Itoa(record.FuturesFii.TotalLong),
		strconv.Itoa(record.FuturesFii.TotalShort),
		strconv.Itoa(record.FuturesFii.Net),
		strconv.Itoa(record.FuturesFii.NetChange),

		strconv.Itoa(record.FuturesPro.TotalLong),
		strconv.Itoa(record.FuturesPro.TotalShort),
		strconv.Itoa(record.FuturesPro.Net),
		strconv.Itoa(record.FuturesPro.NetChange),

		strconv.Itoa(record.FuturesTotal.TotalLong),
		strconv.Itoa(record.FuturesTotal.TotalShort),
		strconv.Itoa(record.FuturesTotal.Net),
		strconv.Itoa(record.FuturesTotal.NetChange),

		strconv.Itoa(record.FuturesSgxNifty.TotalOi),
		strconv.Itoa(record.FuturesSgxNifty.TotalOiChange),
		strconv.Itoa(record.FuturesSgxNifty.MaxOi),

		strconv.Itoa(record.FuturesSgxBank.TotalOi),
		strconv.Itoa(record.FuturesSgxBank.TotalOiChange),
		strconv.Itoa(record.FuturesSgxBank.MaxOi),

		strconv.Itoa(record.OptionsFii.NetCall),
		strconv.Itoa(record.OptionsFii.NetPut),
		strconv.Itoa(record.OptionsFii.Net),
		strconv.Itoa(record.OptionsFii.NetChange),

		strconv.Itoa(record.OptionsPro.NetCall),
		strconv.Itoa(record.OptionsPro.NetPut),
		strconv.Itoa(record.OptionsPro.Net),
		strconv.Itoa(record.OptionsPro.NetChange),

		strconv.Itoa(record.OptionsTotal.NetCall),
		strconv.Itoa(record.OptionsTotal.NetPut),
		strconv.Itoa(record.OptionsTotal.Net),
		strconv.FormatFloat(record.OptionsTotal.Pcr, 'f', 2, 64),
		strconv.Itoa(record.OptionsTotal.NetChange),

		strconv.Itoa(record.OptionsSgxNifty.TotalCall),
		strconv.Itoa(record.OptionsSgxNifty.TotalPut),
		strconv.Itoa(record.OptionsSgxNifty.Net),
		strconv.FormatFloat(record.OptionsSgxNifty.Pcr, 'f', 2, 64),
		strconv.Itoa(record.OptionsSgxNifty.NetChange),
		strconv.Itoa(record.OptionsSgxNifty.MaxCallOi),
		strconv.Itoa(record.OptionsSgxNifty.MaxPutOi),
	})

	if err != nil {
		return err
	}

	self.records = append(self.records, *record)
	return nil
}

func (self *NseFOStats) GetRecordsForDateRange(
	date time.Time,
	numDays int) ([]NseFOStatsRecord, error) {

	if numDays <= 0 {
		return nil, errors.New("numDays must be a positive integer")
	}

	prevDate := date.AddDate(0, 0, -numDays)
	return self.GetRecords(prevDate, date), nil
}

func (self *NseFOStats) GetRecords(
	startDate time.Time,
	endDate time.Time) []NseFOStatsRecord {

	records := []NseFOStatsRecord{}
	for _, record := range self.records {
		if record.Date.After(startDate) && record.Date.Before(endDate) {
			records = append(records, record)
		}
	}
	return records
}

func (self *NseFOStats) GetLatestRecord() *NseFOStatsRecord {
	if len(self.records) <= 0 {
		return nil
	}
	return &self.records[len(self.records)-1]
}
