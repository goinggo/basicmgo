// Package mongodb provides support for accessing and executing commands against
// a mongoDB database
package mongodb

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	mongoDBHosts = "ds035428.mongolab.com:35428"
	authDatabase = "goinggo"
	authUserName = "guest"
	authPassword = "welcome"
	testDatabase = "goinggo"
)

const (
	// MasterSession provides direct access to master database.
	MasterSession = "master"

	// MonotonicSession provides reads to slaves.
	MonotonicSession = "monotonic"
)

var (
	// Reference to the mm.
	mm mongoManager
)

type (
	// mongoConfiguration contains settings for initialization.
	mongoConfiguration struct {
		Hosts    string
		Database string
		UserName string
		Password string
	}

	// mongoManager contains dial and session information.
	mongoSession struct {
		mongoDBDialInfo *mgo.DialInfo
		mongoSession    *mgo.Session
	}

	// mongoManager manages a map of session.
	mongoManager struct {
		sessions map[string]mongoSession
	}

	// DBCall defines a type of function that can be used
	// to excecute code against MongoDB.
	DBCall func(*mgo.Collection) error
)

// Startup brings the manager to a running state.
func Startup() error {
	log.Println("MongoDB : Startup : Started")

	// If the system has already been started ignore the call.
	if mm.sessions != nil {
		return nil
	}

	// Pull in the configuration.
	config := mongoConfiguration{
		Hosts:    mongoDBHosts,
		Database: authDatabase,
		UserName: authUserName,
		Password: authPassword,
	}

	// Create the Mongo Manager.
	mm = mongoManager{
		sessions: make(map[string]mongoSession),
	}

	// Log the mongodb connection straps.
	log.Printf("MongoDB : Startup : Info : Hosts[%s]\n", config.Hosts)
	log.Printf("MongoDB : Startup : Info : Database[%s]\n", config.Database)
	log.Printf("MongoDB : Startup : Info : Username[%s]\n", config.UserName)

	hosts := strings.Split(config.Hosts, ",")

	// Create the strong session.
	if err := CreateSession("strong", MasterSession, hosts, config.Database, config.UserName, config.Password); err != nil {
		log.Println("MongoDB : Startup : ERROR :", err)
		return err
	}

	// Create the monotonic session.
	if err := CreateSession("monotonic", MonotonicSession, hosts, config.Database, config.UserName, config.Password); err != nil {
		log.Println("MongoDB : Startup : ERROR :", err)
		return err
	}

	log.Println("MongoDB : Startup : Completed")
	return nil
}

// Shutdown systematically brings the manager down gracefully.
func Shutdown() error {
	log.Println("MongoDB : Shutdown : Started")

	// Close the sessions.
	for _, session := range mm.sessions {
		CloseSession(session.mongoSession)
	}

	log.Println("MongoDB : Shutdown : Completed")
	return nil
}

// CreateSession creates a connection pool for use.
func CreateSession(mode string, sessionName string, hosts []string, databaseName string, username string, password string) error {
	log.Printf("MongoDB : CreateSession : Started : Mode[%s] SessionName[%s] Hosts[%s] DatabaseName[%s] Username[%s]\n", mode, sessionName, hosts, databaseName, username)

	// Create the database object
	mongoSession := mongoSession{
		mongoDBDialInfo: &mgo.DialInfo{
			Addrs:    hosts,
			Timeout:  60 * time.Second,
			Database: databaseName,
			Username: username,
			Password: password,
		},
	}

	// Establish the master session.
	var err error
	mongoSession.mongoSession, err = mgo.DialWithInfo(mongoSession.mongoDBDialInfo)
	if err != nil {
		log.Println("MongoDB : CreateSession : ERROR:", err)
		return err
	}

	switch mode {
	case "strong":
		// Reads and writes will always be made to the master server using a
		// unique connection so that reads and writes are fully consistent,
		// ordered, and observing the most up-to-date data.
		// http://godoc.org/github.com/finapps/mgo#Session.SetMode
		mongoSession.mongoSession.SetMode(mgo.Strong, true)
		break

	case "monotonic":
		// Reads may not be entirely up-to-date, but they will always see the
		// history of changes moving forward, the data read will be consistent
		// across sequential queries in the same session, and modifications made
		// within the session will be observed in following queries (read-your-writes).
		// http://godoc.org/github.com/finapps/mgo#Session.SetMode
		mongoSession.mongoSession.SetMode(mgo.Monotonic, true)
	}

	// Have the session check for errors.
	// http://godoc.org/github.com/finapps/mgo#Session.SetSafe
	mongoSession.mongoSession.SetSafe(&mgo.Safe{})

	// Add the database to the map.
	mm.sessions[sessionName] = mongoSession

	log.Println("MongoDB : CreateSession : Completed")
	return nil
}

// CopyMasterSession makes a copy of the master session for client use.
func CopyMasterSession() (*mgo.Session, error) {
	return CopySession(MasterSession)
}

// CopyMonotonicSession makes a copy of the monotonic session for client use.
func CopyMonotonicSession() (*mgo.Session, error) {
	return CopySession(MonotonicSession)
}

// CopySession makes a copy of the specified session for client use.
func CopySession(useSession string) (*mgo.Session, error) {
	log.Printf("MongoDB : CopySession : Started : UseSession[%s]\n", useSession)

	// Find the session object.
	session := mm.sessions[useSession]

	if session.mongoSession == nil {
		err := fmt.Errorf("Unable To Locate Session %s", useSession)
		log.Println("MongoDB : CopySession : ERROR :", err)
		return nil, err
	}

	// Copy the master session.
	mongoSession := session.mongoSession.Copy()

	log.Println("MongoDB : CopySession : Completed")
	return mongoSession, nil
}

// CloneMasterSession makes a clone of the master session for client use.
func CloneMasterSession() (*mgo.Session, error) {
	return CloneSession(MasterSession)
}

// CloneMonotonicSession makes a clone of the monotinic session for client use.
func CloneMonotonicSession() (*mgo.Session, error) {
	return CloneSession(MonotonicSession)
}

// CloneSession makes a clone of the specified session for client use.
func CloneSession(useSession string) (*mgo.Session, error) {
	log.Printf("MongoDB : CloneSession : Started : UseSession[%s]\n", useSession)

	// Find the session object.
	session := mm.sessions[useSession]

	if session.mongoSession == nil {
		err := fmt.Errorf("Unable To Locate Session %s", useSession)
		log.Println("MongoDB : CloneSession ERROR :", err)
		return nil, err
	}

	// Clone the master session.
	mongoSession := session.mongoSession.Clone()

	log.Println("MongoDB : CloneSession : Completed")
	return mongoSession, nil
}

// CloseSession puts the connection back into the pool.
func CloseSession(mongoSession *mgo.Session) {
	log.Println("MongoDB : CloseSession : Started")
	mongoSession.Close()
	log.Println("MongoDB : CloseSession : Completed")
}

// GetDatabase returns a reference to the specified database.
func GetDatabase(mongoSession *mgo.Session, useDatabase string) *mgo.Database {
	return mongoSession.DB(useDatabase)
}

// GetCollection returns a reference to a collection for the specified database and collection name.
func GetCollection(mongoSession *mgo.Session, useDatabase string, useCollection string) *mgo.Collection {
	return mongoSession.DB(useDatabase).C(useCollection)
}

// CollectionExists returns true if the collection name exists in the specified database.
func CollectionExists(mongoSession *mgo.Session, useDatabase string, useCollection string) bool {
	database := mongoSession.DB(useDatabase)
	collections, err := database.CollectionNames()

	if err != nil {
		return false
	}

	for _, collection := range collections {
		if collection == useCollection {
			return true
		}
	}

	return false
}

// ToString converts the quer map to a string.
func ToString(queryMap interface{}) string {
	json, err := json.Marshal(queryMap)
	if err != nil {
		return ""
	}

	return string(json)
}

// ToStringD converts bson.D to a string.
func ToStringD(queryMap bson.D) string {
	json, err := json.Marshal(queryMap)
	if err != nil {
		return ""
	}

	return string(json)
}

// Execute the MongoDB literal function.
func Execute(mongoSession *mgo.Session, databaseName string, collectionName string, dbCall DBCall) error {
	log.Printf("MongoDB : Execute : Started : Database[%s] Collection[%s]\n", databaseName, collectionName)

	// Capture the specified collection.
	collection := GetCollection(mongoSession, databaseName, collectionName)
	if collection == nil {
		err := fmt.Errorf("Collection %s does not exist", collectionName)
		log.Println("MongoDB : Execute : ERROR :", err)
		return err
	}

	// Execute the MongoDB call.
	err := dbCall(collection)
	if err != nil {
		log.Println("MongoDB : Execute : ERROR :", err)
		return err
	}

	log.Println("MongoDB : Execute : Completed")
	return nil
}
