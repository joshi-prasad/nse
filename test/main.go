package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/golang/glog"
	"github.com/joshi-prasad/nse"
)

var kUpdateFuturesData = flag.Bool(
	"update_futures_data",
	false,
	"Update the futures data.")

func main() {
	flag.Set("alsologtostderr", "true")
	flag.Parse()

	// strikeCeOi := map[int32][]opts.LineData{}
	// strikePeOi := map[int32][]opts.LineData{}
	// strikeCeChangeOi := map[int32][]opts.LineData{}
	// strikePeChangeOi := map[int32][]opts.LineData{}
	// underlyingAssetPrices := []opts.LineData{}
	// xaxis := []string{}

	nseObj := nse.NewNSE()
	fmt.Println(nseObj)

	if *kUpdateFuturesData == true {
		UpdateFOData(nseObj)
		return
	}

	// webServer := NewWebServer(8080)
	// webServer.Serve()

	for {
		oc, err := nseObj.FetchBankNiftyOc("01-Jun-2023")
		if err != nil {
			glog.Error("Failed to fetch bank nifty OC.", err)
			return
		}

		fmt.Println("==============================================")
		fmt.Println("Time ", time.Now())
		fmt.Println("Underlying Value ", oc.UnderlyingValue())
		fmt.Println("ATM Strike Price ", oc.AtmStrike())
		fmt.Println("Total PCR is ", oc.Pcr())
		fmt.Println("TotalCeOi ", oc.TotalCeOi())
		fmt.Println("TotalPeOi ", oc.TotalPeOi())
		fmt.Println("==============================================")

		strikes32 := oc.GetAtmStrikes(int32(16))
		ocShort := oc.GetOptionChainShortData(strikes32)
		ocShort.RankOi()
		ocShort.RankOiChange()
		ocShort.RankVolume()
		ocShort.WeightedRank()
		ocShort.PrintTable()

		// xaxis = append(xaxis, time.Now().Format("15:04"))
		// underlyingAssetPrices = append(
		// 	underlyingAssetPrices,
		// 	opts.LineData{Value: oc.UnderlyingValue()})
		// for _, oc := range ocShort.Oc {
		// 	strike := oc.Strike
		// 	AppendToStrike(strike, float64(oc.CeOpenInterest), strikeCeOi)
		// 	AppendToStrike(strike, float64(oc.PeOpenInterest), strikePeOi)
		// 	AppendToStrike(strike, float64(oc.CeChangeOpenInterest), strikeCeChangeOi)
		// 	AppendToStrike(strike, float64(oc.PeChangeOpenInterest), strikePeChangeOi)
		// }

		// strikes8 := oc.GetAtmStrikes(int32(8))
		// graphs := []*LineGraph{}
		// graph := NewLineGraph("Title")
		// graphs = append(graphs, graph)
		// for _, strike := range strikes8 {
		// 	graph.SetXAxis(xaxis)
		// 	graph.AddYAxis(fmt.Sprintf("%d_ce_oi", strike), strikeCeOi[strike])
		// 	graph.AddYAxis(fmt.Sprintf("%d_pe_oi", strike), strikePeOi[strike])
		// }
		// webServer.SetLineGraphs(graphs)

		time.Sleep(3 * time.Minute)
	}
}

func UpdateFOData(nseObj *nse.NSE) {
	filePath := "fo_daily_data.csv"
	foStats := nse.NewNseFOStats(filePath)
	if fileStat, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			glog.Info("Creating FO file.")
			fd, err := os.Create(filePath)
			defer fd.Close()
			if err != nil {
				glog.Fatal("Failed to create FO file.")
			}
		} else {
			glog.Fatal("Failed to check if the FO file exists.")
		}
	} else if fileStat.Size() > 0 {
		err := foStats.ReadFile()
		if err != nil {
			glog.Fatal("Failed to read FO file.", err)
		}
	}

	startDate := time.Date(2023, time.May, 1, 0, 0, 0, 0, time.Local)
	endDate := time.Now()
	prevDayRecord := foStats.GetLatestRecord()
	if prevDayRecord == nil {
		startDate = time.Date(endDate.Year(), endDate.Month(), 1, 0, 0, 0, 0, time.Local)
	} else {
		startDate = prevDayRecord.Date.AddDate(0, 0, 1)
	}

	// Loop over the dates and construct a time.Time object for each day
	for date := startDate; date.Before(endDate); date = date.AddDate(0, 0, 1) {
		records, err := nseObj.FetchFOParticipantData(date)
		if err != nil {
			msg := fmt.Sprintf(
				"Failed to fetch F&O participants data for date=%v,err=%v",
				date, err)
			glog.Error(msg)
			continue
		}
		glog.Info("GOT Records ", records)
		prevDayRecord = foStats.GetLatestRecord()

		statsRecord := &nse.NseFOStatsRecord{
			Date:            date,
			FuturesDii:      nse.NseFuturesRecord{},
			FuturesFii:      nse.NseFuturesRecord{},
			FuturesPro:      nse.NseFuturesRecord{},
			FuturesTotal:    nse.NseFuturesRecord{},
			FuturesSgxNifty: nse.SgxFuturesRecord{},
			FuturesSgxBank:  nse.SgxFuturesRecord{},
			OptionsFii:      nse.NseOptionsRecord{},
			OptionsPro:      nse.NseOptionsRecord{},
			OptionsTotal:    nse.NseOptionsRecord{},
			OptionsSgxNifty: nse.SgxOptionsRecord{},
		}
		record := FindClientRecord(records, "DII")
		glog.Info("DII record ", record)
		if record != nil {
			var prevFu *nse.NseFuturesRecord
			if prevDayRecord != nil {
				prevFu = &prevDayRecord.FuturesDii
			}
			statsRecord.FuturesDii.Fill(record, prevFu)
		}
		record = FindClientRecord(records, "FII")
		glog.Info("FII record ", record)
		if record != nil {
			var prevFu *nse.NseFuturesRecord
			if prevDayRecord != nil {
				prevFu = &prevDayRecord.FuturesFii
			}
			statsRecord.FuturesFii.Fill(record, prevFu)

			var prevOp *nse.NseOptionsRecord
			if prevDayRecord != nil {
				prevOp = &prevDayRecord.OptionsFii
			}
			statsRecord.OptionsFii.Fill(record, prevOp)
		}
		record = FindClientRecord(records, "Pro")
		glog.Info("Pro record ", record)
		if record != nil {
			var prevFu *nse.NseFuturesRecord
			if prevDayRecord != nil {
				prevFu = &prevDayRecord.FuturesPro
			}
			statsRecord.FuturesPro.Fill(record, prevFu)

			var prevOp *nse.NseOptionsRecord
			if prevDayRecord != nil {
				prevOp = &prevDayRecord.OptionsPro
			}
			statsRecord.OptionsPro.Fill(record, prevOp)
		}

		totalFut := &statsRecord.FuturesTotal
		totalFut.TotalLong = statsRecord.FuturesDii.TotalLong +
			statsRecord.FuturesFii.TotalLong +
			statsRecord.FuturesPro.TotalLong
		totalFut.TotalShort = statsRecord.FuturesDii.TotalShort +
			statsRecord.FuturesFii.TotalShort +
			statsRecord.FuturesPro.TotalShort
		totalFut.Net = totalFut.TotalLong - totalFut.TotalShort
		if prevDayRecord != nil {
			prev := &prevDayRecord.FuturesTotal
			totalFut.NetChange = totalFut.Net - prev.Net
		}

		totalOp := &statsRecord.OptionsTotal
		totalOp.TotalCallLong = statsRecord.OptionsFii.TotalCallLong +
			statsRecord.OptionsPro.TotalCallLong
		totalOp.TotalCallShort = statsRecord.OptionsFii.TotalCallShort +
			statsRecord.OptionsPro.TotalCallShort
		totalOp.TotalPutLong = statsRecord.OptionsFii.TotalPutLong +
			statsRecord.OptionsPro.TotalPutLong
		totalOp.TotalPutShort = statsRecord.OptionsFii.TotalPutShort +
			statsRecord.OptionsPro.TotalPutShort
		if prevDayRecord != nil {
			prev := &prevDayRecord.OptionsTotal
			totalOp.FillNetValues(prev)
		}

		glog.Info("WRITING record ", statsRecord)
		err = foStats.AppendToFile(statsRecord)
		if err != nil {
			glog.Fatal("Failed to write data ", err)
		}
		time.Sleep(30 * time.Second)
	}
}

func FindClientRecord(
	records []nse.NseFODataRecord, clientType string) *nse.NseFODataRecord {

	for _, record := range records {
		if record.ClientType == clientType {
			return &record
		}
	}
	return nil
}

func AppendToStrike(strike int32, value float64, dst map[int32][]opts.LineData) {
	if _, ok := dst[strike]; !ok {
		dst[strike] = []opts.LineData{}
	}
	dst[strike] = append(dst[strike], opts.LineData{Value: value})
}
