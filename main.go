// This program provides a sample application for using MongoDB with
// the mgo driver.
package main

import (
	"log"
	"sync"

	"github.com/goinggo/basicmgo/mongodb"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type (
	// BuoyCondition contains information for an individual station.
	BuoyCondition struct {
		WindSpeed     float64 `bson:"wind_speed_milehour"`
		WindDirection int     `bson:"wind_direction_degnorth"`
		WindGust      float64 `bson:"gust_wind_speed_milehour"`
	}

	// BuoyLocation contains the buoy's location.
	BuoyLocation struct {
		Type        string    `bson:"type"`
		Coordinates []float64 `bson:"coordinates"`
	}

	// BuoyStation contains information for an individual station.
	BuoyStation struct {
		ID        bson.ObjectId `bson:"_id,omitempty"`
		StationID string        `bson:"station_id"`
		Name      string        `bson:"name"`
		LocDesc   string        `bson:"location_desc"`
		Condition BuoyCondition `bson:"condition"`
		Location  BuoyLocation  `bson:"location"`
	}
)

// Create a wait group to manage the goroutines.
var waitGroup sync.WaitGroup

// main is the entry point for the application.
func main() {
	err := mongodb.Startup()
	if err != nil {
		return
	}

	// Perform 10 concurrent queries against the database.
	waitGroup.Add(10)
	for query := 0; query < 10; query++ {
		go RunQuery(query)
	}

	// Wait for all the queries to complete.
	waitGroup.Wait()
	log.Println("All Queries Completed")
}

// RunQuery is a function that is launched as a goroutine to perform
// the MongoDB work.
func RunQuery(query int) {
	// Decrement the wait group count so the program knows this
	// has been completed once the goroutine exits.
	defer waitGroup.Done()

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	session, err := mongodb.CopyMonotonicSession()
	if err != nil {
		return
	}

	defer session.Close()

	var buoyStations []BuoyStation
	if err := mongodb.Execute(session, "goinggo", "buoy_stations",
		func(collection *mgo.Collection) error {
			return collection.Find(nil).All(&buoyStations)
		}); err != nil {
		log.Println("Error Retrieve maxmind record", err)
		return
	}

	log.Printf("%+v", buoyStations)
}
