package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
)



func parseCmd(args []string) (string, error) {
    
    cmd := args[1:]
    l := len(cmd)

    if l > 2 {
        return "", errors.New(fmt.Sprintf("Too many args provided %v, usage: jbin <filepath>", args))
    }
     
    return args[1], nil

}

type bracket struct {
    open string
    close string
    cntOpen int64
    cntClose int64
}

/*
Iterate cntOpen field by 1.
*/
func (b *bracket) addOpen() {
    b.cntOpen += 1
}

/*
Iterate cntClose field by 1.
*/
func (b *bracket) addClose() {
    b.cntClose += 1
}

/*
Reset cntOpen field to 0.
*/
func (b *bracket) resetOpen() {
    b.cntOpen = 0
}

/*
Reset cntClose field to 0.
*/
func (b *bracket) resetClose() {
    b.cntClose = 0
}

/* 
Creates a new bracket struct.  

'cntOpen' and 'cntClose' fields are set to 0 on initialization.
*/
func newBracket(open string, close string) *bracket {
    return &bracket{open, close, 0, 0}
}

func iterateBracketCount(t json.Token, brack *bracket, curly *bracket) {
    switch t {
    case "[":
        brack.addOpen()
    case "]":
        brack.addClose()
    case "{":
        curly.addOpen()
    case "}":
        curly.addClose()
    }
}

func writeToDisk(c chan, file *os.File) {
// iterate over buffered channel and write values to file
}



func main() {

    var path string
    var file *os.File
    var err error
    
    if path, err = parseCmd(os.Args); err != nil {
        fmt.Println(err)
        os.Exit(1)
    } 
    
    if file, err = os.Open(path); err != nil {
        fmt.Println("Error opening file: ", err)
    }
    defer file.Close()

    // make new file
    // newFile := os.Create()

    decoder := json.NewDecoder(file)
    
    brack := newBracket("[", "]")
    curly := newBracket("{", "}")
    key := true
    firstDelim := true
    var keyLen uint8
    var valLen uint64
    var valBuffer bytes.Buffer
    // need buffered channel and go routine for monitoring channel and writing to file
    
    for {
        if firstDelim {
            firstDelim = false
            continue
        }
        
        token, err := decoder.Token()
        if err == io.EOF {
            break
        }

        if err != nil {
            fmt.Println("Error streaming json: ", err)
            os.Exit(1)
        }
        
        tokentype := fmt.Sprintf("%T", reflect.TypeOf(token))
        
        if !key {
            if tokentype == "json.Delim"{
                iterateBracketCount(token, brack, curly)
            }

            valBytes := token.([]byte)
            valLen = uint64(len(valBytes))
            // add token to valBuffer here
            
            if tokentype != "json.Delim" || token == brack.close || token == curly.close {   
                
                if brack.cntOpen == brack.cntClose && curly.cntOpen == curly.cntClose {
            //      grab value and length of value from  valBuffer
            //      stream valueLength and then value into channel
                    
                    key = true
                    brack.resetOpen()
                    brack.resetClose()
                    curly.resetOpen()
                    curly.resetClose()
                } else {
                    // add delimiter "," to buffer
                }

            }

        } else {
            if tokentype == "json.Delim"{
                continue
            }

            keyBytes := token.([]byte)
            keyLen = uint8(len(keyBytes))
            // stream keyLength and then keyBytes into channel
            key = false
        }

    }


    // TODO
    
    // read JSON file key by key? or iteratively in some way
    //      - json decoder and token

    // convert to binary encoding
    //      - keyLength|key|valueLength|value
    //      - all values would be converted to UTF8 strings

    // stream into a new file

}



