package main

import (
	"errors"
	"fmt"
	"math"

	"gonum.org/v1/gonum/stat/distuv"
)

type OptionGreeks struct {
	Delta  float64
	Delta2 float64
	Theta  float64
	Rho    float64
	Vega   float64
	Gamma  float64
	IV     float64
}

type BlackSchools struct {
	AssetPrice   float64
	StrikePrice  float64
	InterestRate float64
	DaysToExpiry float64
	Volatility   float64
	CePrice      float64
	PePrice      float64

	CeGreeks      OptionGreeks
	PeGreeks      OptionGreeks
	PutCallParity float64
}

func NewBlackSchools(
	assetPrice float64,
	strikePrice float64,
	interestRate float64,
	daysToExpiry float64,
	volatility float64,
	cePrice float64,
	pePrice float64) *BlackSchools {

	return &BlackSchools{
		AssetPrice:   assetPrice,
		StrikePrice:  strikePrice,
		InterestRate: interestRate / 100,
		DaysToExpiry: daysToExpiry / 365,
		Volatility:   volatility / 100,
		CePrice:      cePrice,
		PePrice:      pePrice,
		CeGreeks: OptionGreeks{
			Delta:  0,
			Delta2: 0,
			Theta:  0,
			Rho:    0,
			Vega:   0,
			Gamma:  0,
			IV:     0,
		},
		PeGreeks: OptionGreeks{
			Delta:  0,
			Delta2: 0,
			Theta:  0,
			Rho:    0,
			Vega:   0,
			Gamma:  0,
			IV:     0,
		},
		PutCallParity: 0,
	}
}

// CalculateAValue calculates the value of 'a' used in the Black-Scholes formula.
// 'a' is computed by multiplying the volatility of the option by the square
// root of the number of days to expiry.
// It returns the calculated value of 'a'.
func (self *BlackSchools) CalculateAValue(volatility float64) float64 {
	return volatility * math.Sqrt(self.DaysToExpiry)
}

// CalculateD1Value calculates the value of 'd1' in the Black-Scholes formula.
// It involves several calculations based on the instance variables of the
// BlackSchools struct. It returns the calculated value of 'd1'.
func (self *BlackSchools) CalculateD1Value(volatility float64) float64 {
	// (math.Log(self.AssetPrice/self.StrikePrice): It calculates the natural
	// logarithm of the ratio of the asset price to the strike price.
	// This term represents the logarithmic return of the asset.

	// (self.InterestRate+math.Pow(volatility, 2)/2)*self.DaysToExpiry):
	// It calculates the sum of the interest rate and half of the square of the
	// volatility, multiplied by the number of days to expiry. This term
	// represents the risk premium associated with the option.

	// /self.CalculateAValue(): It divides the sum of the previous terms by the
	// value calculated by the CalculateAValue() function. This value, denoted
	// as 'a' in the Black-Scholes formula, represents the standard deviation
	// of the asset's returns over the period.

	// fmt.Println("self.interestRate ", self.InterestRate)
	// fmt.Println("math.Log(self.AssetPrice/self.StrikePrice) ", math.Log(self.AssetPrice/self.StrikePrice))
	// fmt.Println("(self.InterestRate+math.Pow(volatility, 2)/2) ", (self.InterestRate + math.Pow(volatility, 2)/2))
	// fmt.Println("(math.Log(self.AssetPrice/self.StrikePrice) +"+
	// 	"(self.InterestRate+math.Pow(volatility, 2)/2)*self.DaysToExpiry)", (math.Log(self.AssetPrice/self.StrikePrice) +
	// 	(self.InterestRate+math.Pow(volatility, 2)/2)*self.DaysToExpiry))
	return (math.Log(self.AssetPrice/self.StrikePrice) +
		(self.InterestRate+math.Pow(volatility, 2)/2)*self.DaysToExpiry) /
		self.CalculateAValue(volatility)
}

// CalculateD2Value calculates the value of 'd2' in the Black-Scholes formula.
// It involves several calculations based on the instance variables of the
// BlackSchools struct. It returns the calculated value of 'd2'.
func (self *BlackSchools) CalculateD2Value(volatility float64) float64 {
	// The function calls two other methods: CalculateD1Value and
	// CalculateAValue. It subtracts the value returned by CalculateAValue from
	// the value returned by CalculateD1Value. This calculation represents the
	// difference between the standard deviation of the asset's returns over the
	// period (d1) and the standard deviation adjusted for the time to
	// expiration (a).

	// The resulting value of d2 is used in the Black-Scholes formula to
	// calculate the probability that the option will be exercised. It plays a
	// crucial role in determining the option's price and its sensitivity to
	// changes in other variables like the underlying asset price, strike price,
	// volatility, and time to expiration.
	return self.CalculateD1Value(volatility) - self.CalculateAValue(volatility)
}

// Calculate the cumulative distribution function (CDF) of the standard normal
// distribution at a given value. It returns the probability that a random
// variable from a standard normal distribution is less than or equal to the
// specified value.
func (self *BlackSchools) NormCdf(x float64) float64 {
	return distuv.Normal{Mu: 0, Sigma: 1}.CDF(x)
}

// NormPDF calculates the probability density function (PDF) of a standard
// normal distribution at the given value x.
// It uses the `distuv.UnitNormal` distribution from the
// `gonum.org/v1/gonum/stat/distuv` package to compute the PDF.
// The function returns the computed PDF value.
func (self *BlackSchools) NormPDF(x float64) float64 {
	// This function is used to calculate the probability density of the
	// underlying asset's returns. It is commonly used to estimate the
	// likelihood of different asset price scenarios and to calculate option
	// Greeks like vega or rho, which represent the sensitivity of the option's
	// price to changes in volatility or interest rates, respectively.
	normalDist := distuv.UnitNormal
	return normalDist.Prob(x)
}

func (self *BlackSchools) CalculateBValue() float64 {
	// calculate the value of 'b', which is the exponential of the product of
	// the interest rate and the number of days to expiration. This term
	// represents the present value factor for discounting future cash flows.
	return math.Exp(-self.InterestRate * self.DaysToExpiry)
}

func (self *BlackSchools) ComputeGreeks() {
	volatility := self.Volatility
	self.ComputeDelta(volatility)
	self.ComputeDelta2(volatility)
	self.ComputeVega(volatility)
	self.ComputeTheta(volatility)
	self.ComputeRho(volatility)
	self.ComputeGamma(volatility)
	err := self.ComputeIvUsingCePrice()
	if err != nil {
		fmt.Println("Failed to converge IV using CE Price")
		err = self.ComputeIvUsingPePrice()
		if err != nil {
			fmt.Println("Failed to converge IV using PE Price")
		}
	}
}

// The ComputeDelta method calculates the Delta value for the option using the
// Black-Scholes formula. Delta represents the sensitivity of the option's
// price to changes in the underlying asset price.
// The code first checks if the StrikePrice is less than or equal to 0. If it
// is, the function returns an error indicating that the Strike Price cannot
// be 0.
// It then calculates the value of d1 using the CalculateD1Value method, which
// involves several calculations based on the instance variables of the
// BlackSchools struct.
// The method uses the NormCdf function to calculate the cumulative
// distribution function (CDF) of the standard normal distribution for the
// value of d1. The CDF represents the probability that a random variable from
// a standard normal distribution is less than or equal to the given value.
// The computed Delta value is assigned to the CeGreeks.Delta field for the
// call option and to the PeGreeks.Delta field for the put option.
// Finally, the method returns nil to indicate that the computation was
// successful.
func (self *BlackSchools) ComputeDelta(volatility float64) error {
	if self.StrikePrice <= 0 {
		return errors.New("Strike Price cannot be 0")
	}
	d1 := self.CalculateD1Value(volatility)
	// fmt.Println("D1 ", d1)
	self.CeGreeks.Delta = self.NormCdf(d1)
	self.PeGreeks.Delta = self.NormCdf(-d1)
	return nil
}

// The ComputeDelta2 method is used to compute the second-order delta (Delta2)
// for a given option contract using the Black-Scholes formula.
func (self *BlackSchools) ComputeDelta2(volatility float64) error {
	if self.StrikePrice <= 0 {
		return errors.New("Strike Price cannot be 0")
	}

	b := self.CalculateBValue()

	// 'd2' is one of the inputs in the Black-Scholes formula and is used to
	// calculate the cumulative distribution function (CDF) of the standard
	// normal distribution.
	d2 := self.CalculateD2Value(volatility)

	// Compute Delta2 which represents the sensitivity of the second-order price
	// change of the option with respect to changes in the underlying asset
	// price.
	self.CeGreeks.Delta2 = -self.NormCdf(d2) * b
	self.PeGreeks.Delta2 = self.NormCdf(-d2) * b
	return nil
}

// The ComputeVega() method calculates the Vega (sensitivity to volatility)
// for both the call and put options based on the Black-Scholes model, taking
// into account the underlying asset price, volatility, and days to expiry.
func (self *BlackSchools) ComputeVega(volatility float64) error {
	if volatility == 0 || self.DaysToExpiry == 0 {
		self.CeGreeks.Vega = 0
		self.PeGreeks.Vega = 0
		return nil
	}
	if self.StrikePrice <= 0 {
		return errors.New("Strike Price cannot be 0")
	}

	// NormPDF(d1) The probability density function (PDF) of the standard normal
	// distribution evaluated at d1. It measures the sensitivity of the
	// option's value to changes in volatility.

	// sqrt(DaysToExpiry): The square root of the number of days to expiry,
	// used to adjust the vega calculation.

	// / 100: A scaling factor to ensure the vega is expressed per 1% change
	// in volatility.
	d1 := self.CalculateD1Value(volatility)
	vega := self.AssetPrice * self.NormPDF(d1) * math.Sqrt(self.DaysToExpiry) / 100
	self.CeGreeks.Vega = vega
	self.PeGreeks.Vega = vega
	return nil
}

func (self *BlackSchools) ComputeTheta(volatility float64) {
	b := self.CalculateBValue()
	d1 := self.CalculateD1Value(volatility)
	d2 := self.CalculateD2Value(volatility)

	// -self.AssetPrice*self.NormPDF(d1)*volatility/(2*math.Sqrt(self.DaysToExpiry)):
	// This term represents the contribution of the asset price, volatility, and
	// time to expiry in the time decay calculation.

	// self.InterestRate*self.StrikePrice*b*self.NormCDF(d2): This term
	// accounts for the risk-free interest rate, strike price, and probability
	// of the option expiring in-the-money.

	// To express Theta in terms of daily decay, we divide the computed value by
	// 365. This assumes that there are 365 days in a year, representing the
	// average number of trading days in a year. Dividing by 365 allows us to
	// estimate the daily decay in the option's value.
	self.CeGreeks.Theta = (-self.AssetPrice*self.NormPDF(d1)*volatility/
		(2*math.Sqrt(self.DaysToExpiry)) - self.InterestRate*self.StrikePrice*
		b*self.NormCdf(d2)) / 365

	self.PeGreeks.Theta = (-self.AssetPrice*self.NormPDF(d1)*volatility/
		(2*math.Sqrt(self.DaysToExpiry)) + self.InterestRate*self.StrikePrice*
		b*self.NormCdf(d2)) / 365
}

// In the Black-Scholes model, Rho (ρ) represents the sensitivity of an
// option's value to changes in the risk-free interest rate. It measures the
// expected change in the option price for a 1% change in the risk-free
// interest rate.
func (self *BlackSchools) ComputeRho(volatility float64) {
	// b - represents the present value of the risk-free interest rate.
	b := self.CalculateBValue()

	// d2 - represents the probability of the option expiring in-the-money
	d2 := self.CalculateD2Value(volatility)

	// Calculates the Rho for the call option. It multiplies the strike price by
	// the number of days to expiry, the value of "b", and the cumulative
	// distribution function (CDF) of "d2" (representing the probability of the
	// option expiring in-the-money). The result is divided by 100 to adjust the
	// value to represent a 1% change in the interest rate.
	self.CeGreeks.Rho = self.StrikePrice * self.DaysToExpiry * b *
		self.NormCdf(d2) / 100

	// Calculates the Rho for the put option using similar calculations as for
	// the call option, but using the negative value of "d2" in the CDF
	// calculation.
	self.PeGreeks.Rho = self.StrikePrice * self.DaysToExpiry * b *
		self.NormCdf(-d2) / 100

	// Dividing by 100 is done to scale the result appropriately to represent a
	// 1% change in the interest rate. Rho is typically expressed as the change
	// in option price per 1% change in the risk-free interest rate.
}

// The function calculates the option gamma, which measures the rate of change
// of the option's delta in relation to changes in the underlying asset price.
func (self *BlackSchools) ComputeGamma(volatility float64) {
	// The gamma value represents the sensitivity of the option's delta to
	// small changes in the underlying asset price, and this sensitivity is the
	// same for both put and call options at the same strike price.
	gamma := self.NormPDF(self.CalculateD1Value(volatility)) /
		(self.AssetPrice * self.CalculateAValue(volatility))
	self.CeGreeks.Gamma = gamma
	self.PeGreeks.Gamma = gamma
}

// Put-call parity is a principle in options pricing that establishes a
// relationship between the prices of European-style call and put options with
// the same strike price and expiration date. According to put-call parity,
// the difference between the prices of a call option and a put option is
// equal to the difference between the current price of the underlying asset
// and the present value of the strike price.
// Put-call parity is important because it helps ensure that there are no
// arbitrage opportunities in the options market. It provides a relationship
// between the prices of call and put options, allowing traders to compare the
// prices and evaluate their relative value. If put-call parity is violated,
// it could indicate mispricing in the options market, which could be
// exploited by traders to make risk-free profits.
// The put-call parity formula is as follows:
//    C - P = S - (K / (1 + r)^T)
// Where:
// C is the price of the call option
// P is the price of the put option
// S is the current price of the underlying asset
// K is the strike price
// r is the risk-free interest rate
// T is the time to expiration in years
func (self *BlackSchools) ComputePutCallParity() {
	self.PutCallParity = self.CePrice - self.PePrice - self.AssetPrice +
		(self.StrikePrice / math.Pow(1+self.InterestRate, self.DaysToExpiry))
}

func (self *BlackSchools) ComputeOptionPrice(volatility float64) error {
	if self.StrikePrice == 0 {
		return errors.New("Strike price cannot be 0.")
	}
	if volatility == 0 || self.DaysToExpiry == 0 {
		self.CePrice = MaxFloat(0.0, self.AssetPrice-self.StrikePrice)
		self.PePrice = MaxFloat(0.0, self.StrikePrice-self.AssetPrice)
		return nil
	}
	d1 := self.CalculateD1Value(volatility)
	d2 := self.CalculateD2Value(volatility)
	b := self.CalculateBValue()
	self.CePrice = self.AssetPrice*self.NormCdf(d1) - self.StrikePrice*b*self.NormCdf(d2)
	self.PePrice = self.StrikePrice*b*self.NormCdf(-d2) - self.AssetPrice*self.NormCdf(-d1)
	return nil
}

// Calculate the implied volatility (IV) for the given option prices
// (CePrice and PePrice) using the Black-Scholes model. The IV represents the
// market's expectation of the future volatility of the underlying asset based
// on the observed option prices.
// The ComputeIvUsingPePrice() function uses a binary search algorithm to find the IV that
// produces option prices close to the observed prices. It iteratively adjusts
// the IV guess based on the comparison of the calculated option prices with
// the observed prices.
func (self *BlackSchools) ComputeIvUsingPePrice() error {
	const maxIterations = 1000
	const tolerance = 0.0001

	setCePrice := self.CePrice
	setPePrice := self.PePrice
	defer func() {
		fmt.Println("Restoring original prices. ", self.CePrice, self.PePrice)
		self.CePrice = setCePrice
		self.PePrice = setPePrice
	}()

	if setCePrice <= 0 || setPePrice <= 0 {
		return errors.New("Option prices must be positive")
	}

	// Initialize variables for the iteration
	iv := 0.5 // Initial guess for IV
	lowerBound := 0.0
	upperBound := 1.0

	for i := 0; i < maxIterations; i++ {
		// Calculate option prices using the current IV guess
		self.ComputeOptionPrice(iv)
		// bsCePrice := self.CePrice
		bsPePrice := self.PePrice
		// fmt.Println(bsCePrice, bsPePrice, iv)

		// Check if the calculated option prices are close enough to the observed prices
		if math.Abs(bsPePrice-setPePrice) < tolerance {
			self.CeGreeks.IV = iv
			self.PeGreeks.IV = iv
			return nil // IV calculation successful
		}

		// Adjust the IV guess based on the difference between observed and calculated option prices
		if bsPePrice < setPePrice {
			lowerBound = iv
		} else {
			upperBound = iv
		}

		iv = (lowerBound + upperBound) / 2 // Update IV guess using binary search
	}

	return errors.New("IV calculation did not converge") // IV calculation did not converge within the maximum number of iterations
}

func (self *BlackSchools) ComputeIvUsingCePrice() error {
	const maxIterations = 1000
	const tolerance = 0.0001

	setCePrice := self.CePrice
	setPePrice := self.PePrice
	defer func() {
		fmt.Println("Restoring original prices. ", self.CePrice, self.PePrice)
		self.CePrice = setCePrice
		self.PePrice = setPePrice
	}()

	if setCePrice <= 0 || setPePrice <= 0 {
		return errors.New("Option prices must be positive")
	}

	// Initialize variables for the iteration
	iv := 0.5 // Initial guess for IV
	lowerBound := 0.0
	upperBound := 1.0

	for i := 0; i < maxIterations; i++ {
		// Calculate option prices using the current IV guess
		self.ComputeOptionPrice(iv)
		bsCePrice := self.CePrice
		// bsPePrice := self.PePrice
		// fmt.Println(bsCePrice, bsPePrice, iv)

		// Check if the calculated option prices are close enough to the observed prices
		if math.Abs(bsCePrice-setCePrice) < tolerance {
			self.CeGreeks.IV = iv
			self.PeGreeks.IV = iv
			return nil // IV calculation successful
		}

		// Adjust the IV guess based on the difference between observed and calculated option prices
		if bsCePrice < setCePrice {
			lowerBound = iv
		} else {
			upperBound = iv
		}

		iv = (lowerBound + upperBound) / 2 // Update IV guess using binary search
	}

	return errors.New("IV calculation did not converge") // IV calculation did not converge within the maximum number of iterations
}

func MaxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func main() {
	bs := NewBlackSchools(43392, 43300.0, 8.0, 0.125, 25, 135.05, 35.90)
	// fmt.Println("Vol ", bs.Volatility)
	// fmt.Println("DaysToExpiry ", bs.DaysToExpiry)
	// fmt.Println("A ", bs.CalculateAValue(bs.Volatility))
	// fmt.Println("D1 ", bs.CalculateD1Value(bs.Volatility))
	bs.ComputeGreeks()
	fmt.Println("bs.CeGreeks.Delta ", bs.CeGreeks.Delta)
	fmt.Println("bs.CeGreeks.Delta2 ", bs.CeGreeks.Delta2)
	fmt.Println("bs.CeGreeks.Theta ", bs.CeGreeks.Theta)
	fmt.Println("bs.CeGreeks.Rho ", bs.CeGreeks.Rho)
	fmt.Println("bs.CeGreeks.Vega ", bs.CeGreeks.Vega)
	fmt.Println("bs.CeGreeks.Gamma ", bs.CeGreeks.Gamma)
	fmt.Println("bs.CeGreeks.IV ", bs.CeGreeks.IV)
}
