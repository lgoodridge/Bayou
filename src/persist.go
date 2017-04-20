package bayou

import (
    "io/ioutil"
)


func check(e error) {
    if e != nil {
        panic(e)
    }
}

func save(data []byte, id int) {
     err := ioutil.WriteFile("/tmp/bayou-data." + id, data, 0644)
     check(err)
}

func load(id int) ([]byte, error) {
    dat, err := ioutil.ReadFile("/tmp/bayou-data." + id)
    return dat, err
}
