package nse

import (
	"fmt"
	"sort"

	"github.com/fatih/color"
	"github.com/golang/glog"
)

const (
	kVolumeWeight   = 0.4
	kOiWeight       = 0.4
	kChangeOiWeight = 0.2

	kMaxCombinedWeight = 8.0
)

type NseOcRow struct {
	data map[string]interface{}
}

func NewNseOcRow(data map[string]interface{}) *NseOcRow {
	return &NseOcRow{
		data: data,
	}
}

func (self *NseOcRow) OpenInterest() int64 {
	value, err := getFloat64Field(self.data, kOcRowOpenInterest)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse open interest.")
		glog.Error(msg)
		return 0
	}
	return int64(value)
}

func (self *NseOcRow) ChangeOpenInterest() int64 {
	value, err := getFloat64Field(self.data, kOcRowChangeinOpenInterest)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse change in open interest.")
		glog.Error(msg)
		return 0
	}
	return int64(value)
}

func (self *NseOcRow) Ltp() float64 {
	value, err := getFloat64Field(self.data, kOcRowLastPrice)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse LTP.")
		glog.Error(msg)
		return 0
	}
	return value
}

func (self *NseOcRow) TradedVolume() int64 {
	value, err := getFloat64Field(self.data, kOcRowTotalTradedVolume)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse traded volume.")
		glog.Error(msg)
		return 0
	}
	return int64(value)
}

type NseOcRowData struct {
	StrikePrice int32
	Pe          *NseOcRow
	Ce          *NseOcRow
}

func NewNseOcRowData(
	StrikePrice int32,
	ce map[string]interface{},
	pe map[string]interface{}) *NseOcRowData {

	return &NseOcRowData{
		StrikePrice: StrikePrice,
		Pe:          NewNseOcRow(pe),
		Ce:          NewNseOcRow(ce),
	}
}

type NseOc struct {
	symbol          string
	expiryDate      string
	timestamp       string
	underlyingValue float64
	strikeStep      int32

	// map of strike price to its row data
	rows map[int32]*NseOcRowData

	totalCeOi int64
	totalPeOi int64
	pcr       float64
}

func NewNseOc(
	symbol string,
	expiryDate string,
	timestamp string,
	underlyingValue float64) *NseOc {

	return &NseOc{
		symbol:          symbol,
		expiryDate:      expiryDate,
		timestamp:       timestamp,
		underlyingValue: underlyingValue,
		strikeStep:      0,
		rows:            map[int32]*NseOcRowData{},
		totalCeOi:       0,
		totalPeOi:       0,
		pcr:             0,
	}
}

func (self *NseOc) TotalCeOi() int64 {
	return self.totalCeOi
}

func (self *NseOc) TotalPeOi() int64 {
	return self.totalPeOi
}

func (self *NseOc) Pcr() float64 {
	return self.pcr
}

func (self *NseOc) SetStrikeStep(step int32) {
	self.strikeStep = step
}

func (self *NseOc) SetOcDataRecords(
	dataRecords []map[string]interface{}) {

	for _, record := range dataRecords {
		strikePriceFloat, err := getFloat64Field(record, kOcRecordsDataStrikePrice)
		if err != nil {
			continue
		}
		strikePrice := int32(strikePriceFloat)
		pe, err := getStringToInterfaceMap(record, kOcRecordsDataPe)
		if err != nil {
			msg := fmt.Sprintf("PE row absent for strike=%d", strikePrice)
			glog.Info(msg)
		}
		ce, err := getStringToInterfaceMap(record, kOcRecordsDataCe)
		if err != nil {
			msg := fmt.Sprintf("PE row absent for strike=%d", strikePrice)
			glog.Info(msg)
		}
		self.rows[strikePrice] = NewNseOcRowData(strikePrice, ce, pe)
	}

	self.computeAndSetTotalCeOi()
	self.computeAndSetTotalPeOi()
	self.setPcr()
}

func (self *NseOc) setPcr() {
	if self.totalCeOi <= 0 || self.totalPeOi <= 0 {
		return
	}
	self.pcr = float64(self.totalPeOi) / float64(self.totalCeOi)
}

func (self *NseOc) computeAndSetTotalCeOi() {
	totalOi := int64(0)
	for _, rowData := range self.rows {
		if rowData.Ce == nil {
			continue
		}
		oi := rowData.Ce.OpenInterest()
		if oi <= 0 {
			continue
		}
		totalOi += oi
	}
	self.totalCeOi = totalOi
}

func (self *NseOc) computeAndSetTotalPeOi() {
	totalOi := int64(0)
	for _, rowData := range self.rows {
		if rowData.Pe == nil {
			continue
		}
		oi := rowData.Pe.OpenInterest()
		if oi <= 0 {
			continue
		}
		totalOi += oi
	}
	self.totalPeOi = totalOi
}

func (self *NseOc) AtmStrike() int32 {
	// glog.Error("********* ", self.underlyingValue, self.strikeStep)
	return roundToStep(self.underlyingValue, self.strikeStep)
}

func (self *NseOc) UnderlyingValue() float64 {
	return self.underlyingValue
}

func (self *NseOc) ExpiryDate() string {
	return self.expiryDate
}

func (self *NseOc) GetAtmStrikes(totalStrikes int32) []int32 {
	step := self.strikeStep
	strikes := make([]int32, totalStrikes)
	atm := self.AtmStrike()
	beginValue := atm - ((totalStrikes / 2) * step)
	for ii := int32(0); ii < totalStrikes; ii += 1 {
		strikes[ii] = beginValue + (ii * step)
	}
	return strikes
}

func (self *NseOc) NseOcForStrikes(strikes []int32) *NseOc {
	oc := NewNseOc(self.symbol, self.expiryDate, self.timestamp,
		self.underlyingValue)

	for _, strike := range strikes {
		oc.rows[strike] = self.rows[strike]
	}
	oc.SetStrikeStep(self.strikeStep)
	oc.computeAndSetTotalCeOi()
	oc.computeAndSetTotalPeOi()
	oc.setPcr()
	return oc
}

type OptionChainShortData struct {
	Strike int32

	CeOpenInterest       int64
	CeChangeOpenInterest int64
	CeTradedVolume       int64
	CeLtp                float64

	PeOpenInterest       int64
	PeChangeOpenInterest int64
	PeTradedVolume       int64
	PeLtp                float64

	PcrOi       float64
	PcrChangeOi float64
	PcrVolume   float64

	CeVolumeRank   int
	CeOiRank       int
	CeChangeOiRank int
	CeWeightedRank float32

	PeVolumeRank   int
	PeOiRank       int
	PeChangeOiRank int
	PeWeightedRank float32
}

type NseShortOc struct {
	UnderlyingValue float64
	AtmStrike       int32
	Oc              []*OptionChainShortData

	TotalCeOi       int64
	TotalCeChangeOi int64
	TotalCeVolume   int64

	TotalPeOi       int64
	TotalPeVolume   int64
	TotalPeChangeOi int64

	PcrOi       float64
	PcrChangeOi float64
	PcrVolume   float64
}

func NewNseShortOc(
	Oc []*OptionChainShortData,
	UnderlyingValue float64,
	AtmStrike int32,
	TotalCeOi int64,
	TotalCeChangeOi int64,
	TotalCeVolume int64,
	TotalPeOi int64,
	TotalPeChangeOi int64,
	TotalPeVolume int64) *NseShortOc {

	return &NseShortOc{
		UnderlyingValue: UnderlyingValue,
		AtmStrike:       AtmStrike,
		Oc:              Oc,
		TotalCeOi:       TotalCeOi,
		TotalCeChangeOi: TotalCeChangeOi,
		TotalCeVolume:   TotalCeVolume,
		TotalPeOi:       TotalPeOi,
		TotalPeVolume:   TotalPeVolume,
		TotalPeChangeOi: TotalPeChangeOi,
		PcrOi:           computePcr(TotalPeOi, TotalCeOi),
		PcrChangeOi:     computePcr(TotalPeChangeOi, TotalCeChangeOi),
		PcrVolume:       computePcr(TotalPeVolume, TotalCeVolume),
	}
}

func (self *OptionChainShortData) String() string {
	return fmt.Sprintf("Strike: %d, CE Open Interest: %d, "+
		"CE Change Open Interest: %d, PE Open Interest: %d, "+
		"PE Change Open Interest: %d",
		self.Strike, self.CeOpenInterest, self.CeChangeOpenInterest,
		self.PeOpenInterest, self.PeChangeOpenInterest)
}

func (self *NseOc) GetStrikes(beginStrike int32, totalStrikes int32) []int32 {
	step := self.strikeStep
	strikes := make([]int32, totalStrikes)
	for ii := int32(0); ii < totalStrikes; ii += 1 {
		strikes[ii] = beginStrike + (ii * step)
	}
	return strikes
}

func (self *NseOc) GetOptionChainShortData(strikes []int32) *NseShortOc {
	totalCeOi := int64(0)
	totalCeVolume := int64(0)
	totalCeChangeOi := int64(0)

	totalPeOi := int64(0)
	totalPeVolume := int64(0)
	totalPeChangeOi := int64(0)

	shortOcData := []*OptionChainShortData{}
	for _, strike := range strikes {
		row, ok := self.rows[strike]
		if !ok {
			continue
		}

		data := &OptionChainShortData{
			Strike:               strike,
			CeOpenInterest:       0,
			CeChangeOpenInterest: 0,
			CeTradedVolume:       0,
			CeLtp:                0,
			PeOpenInterest:       0,
			PeChangeOpenInterest: 0,
			PeTradedVolume:       0,
			PeLtp:                0,
			PcrOi:                0,
			PcrChangeOi:          0,
			PcrVolume:            0,
		}
		if ce := row.Ce; ce != nil {
			data.CeOpenInterest = ce.OpenInterest()
			data.CeChangeOpenInterest = ce.ChangeOpenInterest()
			data.CeTradedVolume = ce.TradedVolume()
			data.CeLtp = ce.Ltp()
		}
		if pe := row.Pe; pe != nil {
			data.PeOpenInterest = pe.OpenInterest()
			data.PeChangeOpenInterest = pe.ChangeOpenInterest()
			data.PeTradedVolume = pe.TradedVolume()
			data.PeLtp = pe.Ltp()
		}
		data.PcrOi = computePcr(data.PeOpenInterest, data.CeOpenInterest)
		data.PcrChangeOi = computePcr(data.PeChangeOpenInterest, data.CeChangeOpenInterest)
		data.PcrVolume = computePcr(data.PeTradedVolume, data.CeTradedVolume)

		totalCeOi += data.CeOpenInterest
		totalCeVolume += data.CeTradedVolume
		totalCeChangeOi += data.CeChangeOpenInterest

		totalPeOi += data.PeOpenInterest
		totalPeVolume += data.PeTradedVolume
		totalPeChangeOi += data.PeChangeOpenInterest

		shortOcData = append(shortOcData, data)
	}

	// Sort the result slice based on the Strike field
	sort.Slice(shortOcData, func(i, j int) bool {
		return shortOcData[i].Strike < shortOcData[j].Strike
	})

	return NewNseShortOc(
		shortOcData,
		self.UnderlyingValue(),
		self.AtmStrike(),
		totalCeOi,
		totalCeChangeOi,
		totalCeVolume,
		totalPeOi,
		totalPeChangeOi,
		totalPeVolume)
}

func (self *NseShortOc) PrintTable() {
	data := self.Oc

	// Print table headers
	fmt.Printf("%-10s %-6s %-12s %-12s %-10s %-10s %-12s %-6s %-10s %s"+
		" %-10s %-10s %-10s || %-8s %-8s %-8s || %-8s %-8s %-8s || %-8s %-8s ||\n",
		"CeLtp", "CeOi", "CeChangeOi", "CeVolume", "Strike", "PeVolume",
		"PeChangeOi", "PeOi", "PeLtp", "||", "PcrOi", "PcrVolume", "PcrChangeOi",
		"CE_VR", "CE_OIR", "CE_COIR", "PE_VR", "PE_OIR", "PE_COIR",
		"CE_RANK", "PE_RANK")

	// Set color for ITM data
	yellowColor := color.New(color.FgYellow).SprintFunc()
	defaultColor := color.New(color.FgBlue).SprintFunc()
	greenColor := color.New(color.FgGreen).SprintFunc()
	redColor := color.New(color.FgRed).SprintFunc()

	// Print CE and PE values, along with the additional Pcr columns
	for _, row := range data {
		atmChar := ' '
		if row.Strike == self.AtmStrike {
			atmChar = '*'
		}

		ceColor := yellowColor
		peColor := yellowColor
		if row.Strike < self.AtmStrike {
			peColor = defaultColor
		} else {
			ceColor = defaultColor
		}

		ceRankColor := redColor
		peRankColor := redColor
		if row.CeWeightedRank <= (float32(len(self.Oc)) / 2.8) {
			ceRankColor = greenColor
		}
		if row.PeWeightedRank <= (float32(len(self.Oc)) / 2.8) {
			peRankColor = greenColor
		}

		// Print CE and PE values with color formatting
		fmt.Printf("%s %c %-10d %s %s %-10.2f %-10.2f %-10.2f %s %-8d %-8d %-8d "+
			"%s %-8d %-8d %-8d %-2s %-10s %-10s\n",
			ceColor(
				fmt.Sprintf("%-10.2f %-6d %-12d %-12d", row.CeLtp, row.CeOpenInterest,
					row.CeChangeOpenInterest, row.CeTradedVolume)),
			atmChar, row.Strike,
			peColor(
				fmt.Sprintf("%-10d %-12d %-6d %-10.2f", row.PeTradedVolume,
					row.PeChangeOpenInterest, row.PeOpenInterest, row.PeLtp)),
			"||", row.PcrOi, row.PcrVolume, row.PcrChangeOi,
			"||", row.CeVolumeRank, row.CeOiRank, row.CeChangeOiRank,
			"||", row.PeVolumeRank, row.PeOiRank, row.PeChangeOiRank,
			"||", ceRankColor(fmt.Sprintf("%-0.1f", row.CeWeightedRank)),
			peRankColor(fmt.Sprintf("%-0.1f", row.PeWeightedRank)))
	}

	// Print the total values
	fmt.Println("\nTotals:")
	fmt.Printf("Total CE Open Interest:       %-10d\n", self.TotalCeOi)
	fmt.Printf("Total CE Change Open Interest:%-10d\n", self.TotalCeChangeOi)
	fmt.Printf("Total CE Traded Volume:       %-10d\n", self.TotalCeVolume)
	fmt.Printf("Total PE Open Interest:       %-10d\n", self.TotalPeOi)
	fmt.Printf("Total PE Change Open Interest:%-10d\n", self.TotalPeChangeOi)
	fmt.Printf("Total PE Traded Volume:       %-10d\n", self.TotalPeVolume)
	fmt.Println("------------------------------")
	fmt.Printf("PCR OI:                      %-10f\n", self.PcrOi)
	fmt.Printf("PCR Volume:                 %-10f\n", self.PcrVolume)
	fmt.Printf("PCR Change OI:              %-10f\n", self.PcrChangeOi)
}

func computePcr(pe int64, ce int64) float64 {
	if ce <= 0 {
		return 0
	}
	return float64(pe) / float64(ce)
}

func (self *NseShortOc) RankVolume() {
	oc := make([]*OptionChainShortData, len(self.Oc))
	copy(oc, self.Oc[:])

	sort.Slice(oc, func(i, j int) bool {
		return oc[i].CeTradedVolume >= oc[j].CeTradedVolume
	})
	for rank, oc := range oc {
		oc.CeVolumeRank = rank + 1
	}

	copy(oc, self.Oc[:])
	sort.Slice(oc, func(i, j int) bool {
		return oc[i].PeTradedVolume >= oc[j].PeTradedVolume
	})
	for rank, oc := range oc {
		oc.PeVolumeRank = rank + 1
	}
}

func (self *NseShortOc) RankOi() {
	oc := make([]*OptionChainShortData, len(self.Oc))
	copy(oc, self.Oc[:])

	sort.Slice(oc, func(i, j int) bool {
		return oc[i].CeOpenInterest >= oc[j].CeOpenInterest
	})
	for rank, oc := range oc {
		oc.CeOiRank = rank + 1
	}

	copy(oc, self.Oc[:])
	sort.Slice(oc, func(i, j int) bool {
		return oc[i].PeOpenInterest >= oc[j].PeOpenInterest
	})
	for rank, oc := range oc {
		oc.PeOiRank = rank + 1
	}
}

func (self *NseShortOc) RankOiChange() {
	oc := make([]*OptionChainShortData, len(self.Oc))
	copy(oc, self.Oc[:])

	sort.Slice(oc, func(i, j int) bool {
		return oc[i].CeChangeOpenInterest >= oc[j].CeChangeOpenInterest
	})
	for rank, oc := range oc {
		oc.CeChangeOiRank = rank + 1
	}

	copy(oc, self.Oc[:])
	sort.Slice(oc, func(i, j int) bool {
		return oc[i].PeChangeOpenInterest >= oc[j].PeChangeOpenInterest
	})
	for rank, oc := range oc {
		oc.PeChangeOiRank = rank + 1
	}
}

func (self *NseShortOc) WeightedRank() {
	for _, oc := range self.Oc {
		oc.CeWeightedRank = float32(oc.CeOiRank)*kOiWeight +
			float32(oc.CeVolumeRank)*kVolumeWeight +
			float32(oc.CeChangeOiRank)*kChangeOiWeight

		oc.PeWeightedRank = float32(oc.PeOiRank)*kOiWeight +
			float32(oc.PeVolumeRank)*kVolumeWeight +
			float32(oc.PeChangeOiRank)*kChangeOiWeight
	}
}
