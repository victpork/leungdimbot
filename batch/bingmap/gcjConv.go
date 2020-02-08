package bingmap

import (
	"math"
)

const earthR = 6378137

// GCJtoWGS convert GCJ-02 coordinate(gcjLat, gcjLng) to WGS-84 coordinate(wgsLat, wgsLng).
// The output WGS-84 coordinate's accuracy is 1m to 2m. If you want more exactly result, use GCJtoWGSExact/gcj2wgs_exact.
func GCJtoWGS(gcjLat, gcjLng float64) (wgsLat, wgsLng float64) {
	dLat, dLng := delta(gcjLat, gcjLng)
	wgsLat, wgsLng = gcjLat-dLat, gcjLng-dLng
	return
}

func delta(lat, lng float64) (dLat, dLng float64) {
	const ee = 0.00669342162296594323
	dLat, dLng = transform(lng-105.0, lat-35.0)
	radLat := lat / 180.0 * math.Pi
	magic := math.Sin(radLat)
	magic = 1 - ee*magic*magic
	sqrtMagic := math.Sqrt(magic)
	dLat = (dLat * 180.0) / ((earthR * (1 - ee)) / (magic * sqrtMagic) * math.Pi)
	dLng = (dLng * 180.0) / (earthR / sqrtMagic * math.Cos(radLat) * math.Pi)
	return
}

func transform(x, y float64) (lat, lng float64) {
	xy := x * y
	absX := math.Sqrt(math.Abs(x))
	xPi := x * math.Pi
	yPi := y * math.Pi
	d := 20.0*math.Sin(6.0*xPi) + 20.0*math.Sin(2.0*xPi)

	lat = d
	lng = d

	lat += 20.0*math.Sin(yPi) + 40.0*math.Sin(yPi/3.0)
	lng += 20.0*math.Sin(xPi) + 40.0*math.Sin(xPi/3.0)

	lat += 160.0*math.Sin(yPi/12.0) + 320*math.Sin(yPi/30.0)
	lng += 150.0*math.Sin(xPi/12.0) + 300.0*math.Sin(xPi/30.0)

	lat *= 2.0 / 3.0
	lng *= 2.0 / 3.0

	lat += -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*xy + 0.2*absX
	lng += 300.0 + x + 2.0*y + 0.1*x*x + 0.1*xy + 0.1*absX

	return
}
