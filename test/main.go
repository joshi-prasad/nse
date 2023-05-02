package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/joshi-prasad/nse"
)

func main() {
	flag.Set("alsologtostderr", "true")
	nseObj := nse.NewNSE()
	fmt.Println(nseObj)

	for {
		oc, err := nseObj.FetchBankNiftyOc("02-May-2023")
		if err != nil {
			glog.Error("Failed to fetch bank nifty OC.", err)
			return
		}

		fmt.Println("==============================================")
		fmt.Println("Underlying Value ", oc.UnderlyingValue())
		fmt.Println("ATM Strike Price ", oc.AtmStrike())
		fmt.Println("Total PCR is ", oc.Pcr())
		fmt.Println("TotalCeOi ", oc.TotalCeOi())
		fmt.Println("TotalPeOi ", oc.TotalPeOi())
		fmt.Println("==============================================")

		strikes := oc.GetAtmStrikes(int32(20))
		ocShort := oc.GetOptionChainShortData(strikes)
		ocShort.RankOi()
		ocShort.RankOiChange()
		ocShort.RankVolume()
		ocShort.WeightedRank()
		ocShort.PrintTable()

		time.Sleep(5 * time.Minute)
	}
}
