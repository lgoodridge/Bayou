package bayou

import (
    "errors"
    "fmt"
    "io/ioutil"
    "os"
)

const PERSIST_FILE_PREFIX string = "/tmp/bayou-data."
const FILE_NOT_FOUND_ERROR string = "File does not exist"

/* Saves provided data to disk under the provided id */
func save(data []byte, id int) {
     filePath := PERSIST_FILE_PREFIX + fmt.Sprintf("%d", id)
     err := ioutil.WriteFile(filePath, data, 0644)
     check(err, "Error writing to file: ")
}

/* Loads saved data for the provided id from disk */
func load(id int) ([]byte, error) {
     filePath := PERSIST_FILE_PREFIX + fmt.Sprintf("%d", id)
     if !fileExists(filePath) {
         return nil, errors.New(FILE_NOT_FOUND_ERROR)
     }
     dat, err := ioutil.ReadFile(filePath)
     return dat, err
}

/* Deletes saved data for the provided id from disk */
func DeletePersist(id int) {
    filePath := PERSIST_FILE_PREFIX + fmt.Sprintf("%d", id)
    if fileExists(filePath) {
        err := os.Remove(filePath)
        check(err, "Error deleting persistent file: ")
    }
}
