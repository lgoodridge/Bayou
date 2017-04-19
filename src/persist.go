package bayou

import (
//    "bytes"
//   "encoding/gob"
    "io/ioutil"
//    "os"
)


func check(e error) {
    if e != nil {
        panic(e)
    }
}

func save(data []byte) {
     err := ioutil.WriteFile("/tmp/bayou-data", data, 0644)
     check(err)
//     f, err := os.Create("/tmp/bayou-data")
//     check(err)
//     defer f.Close()
//
//     f.Write(data)
}

func load() ([]byte, error) {
    dat, err := ioutil.ReadFile("/tmp/bayou-data")
    // check(err)
    return dat, err
}
