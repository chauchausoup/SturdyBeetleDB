package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Info(string, ...interface{})
		Warn(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		rwMutex sync.RWMutex
		mutexes map[string]*sync.RWMutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}
	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := &Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.RWMutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s' ....", dir)
	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save")
	}
	if resource == "" {
		return fmt.Errorf("missing resource - unable to save record (no name)")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	finalPath := filepath.Join(dir, resource+".json")
	tmpPath := finalPath + ".temp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %v", err)
	}

	return os.Rename(tmpPath, finalPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection")
	}
	if resource == "" {
		return fmt.Errorf("missing resource")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.RLock()
	defer mutex.RUnlock()

	recordPath := filepath.Join(d.dir, collection, resource+".json")
	if _, err := os.Stat(recordPath); err != nil {
		return fmt.Errorf("record not found: %v", err)
	}

	b, err := os.ReadFile(recordPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	return json.Unmarshal(b, v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.RLock()
	defer mutex.RUnlock()

	dir := filepath.Join(d.dir, collection)

	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("collection not found: %v", err)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}

	var records []string
	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %v", err)
		}
		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	if collection == "" || resource == "" {
		return fmt.Errorf("collection or resource name is missing")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	path := filepath.Join(d.dir, collection, resource+".json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("resource does not exist")
	}

	return os.Remove(path)
}

func (d *Driver) getOrCreateMutex(collection string) *sync.RWMutex {
	d.rwMutex.Lock()
	defer d.rwMutex.Unlock()

	if m, exists := d.mutexes[collection]; exists {
		return m
	}

	m := &sync.RWMutex{}
	d.mutexes[collection] = m
	return m
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode string
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error occurred:", err)
		return
	}

	employees := []User{
		{"Mrinal", "19", "3423251", "Aramco", Address{"Varanasi", "Up", "India", "3424"}},
		{"Utkarsh", "18", "3423234", "Airtel", Address{"JanakPuri", "Delhi", "India", "8912"}},
		{"Prachi", "17", "3423251", "Aramco", Address{"Bhidaur", "Tamil Nadu", "India", "1321"}},
	}

	for _, emp := range employees {
		if err := db.Write("users", emp.Name, emp); err != nil {
			fmt.Println("Error writing user:", err)
		}
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error occurred:", err)
		return
	}
	fmt.Println("Records:", records)

	var allUsers []User
	for _, record := range records {
		var user User
		if err := json.Unmarshal([]byte(record), &user); err != nil {
			fmt.Println("Error unmarshalling record:", err)
			continue
		}
		allUsers = append(allUsers, user)
	}

	fmt.Println("All Users:", allUsers)

	// Example: Deleting a user
	// if err := db.Delete("users", "Mrinal"); err != nil {
	// 	fmt.Println("Error deleting user:", err)
	// }
}
