package bayou

import (
    "io/ioutil"
    "fmt"
)


func check(e error) {
    if e != nil {
        panic(e)
    }
}

func save(data []byte, id int) {
     stringId := fmt.Sprintf("%d",id)
     err := ioutil.WriteFile("/tmp/bayou-data." + stringId, data, 0644)
     check(err)
}

func load(id int) ([]byte, error) {
     stringId := fmt.Sprintf("%d",id)
     dat, err := ioutil.ReadFile("/tmp/bayou-data." + stringId)
     return dat, err
}
