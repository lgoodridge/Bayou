package bayou

import (
    "io/ioutil"
    "fmt"
)

/* Saves provided data to disk under the provided id */
func save(data []byte, id int) {
     stringId := fmt.Sprintf("%d",id)
     err := ioutil.WriteFile("/tmp/bayou-data." + stringId, data, 0644)
     check(err, "Error writing to file: ")
}

/* Loads saved data for the provided id from disk */
func load(id int) ([]byte, error) {
     stringId := fmt.Sprintf("%d",id)
     dat, err := ioutil.ReadFile("/tmp/bayou-data." + stringId)
     return dat, err
}
