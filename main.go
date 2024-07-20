package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
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

/*
iterate over buffered channel and write values to file
*/
func writeToDisk(c chan([]byte), file *os.File, wg *sync.WaitGroup) {
    
    for m := range c {
        
        file.Write(m)
        wg.Done()

    }

}



func main() {

    var path string
    var file *os.File
    var newFile *os.File
    var err error
    var wg *sync.WaitGroup



    if path, err = parseCmd(os.Args); err != nil {
        fmt.Println(err)
        os.Exit(1)
    } 
    
    if file, err = os.Open(path); err != nil {
        fmt.Println("Error opening file: ", err)
    }
    defer file.Close()

    // make new file
    if newFile, err = os.Create(strings.Trim(path, ".json")+".bin"); err != nil {
        fmt.Println("Error creating new file", err)
    }


    decoder := json.NewDecoder(file)
    
    brack := newBracket("[", "]")
    curly := newBracket("{", "}")
    key := true
    firstDelim := true
    var keyLen uint8
    var valLen uint64
    var valBuffer bytes.Buffer
    var keyBuffer bytes.Buffer
    
    writeChannel := make(chan []byte) 
    
    go writeToDisk(writeChannel, newFile, wg)
    
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
 
            valBytes := token.([]byte)
            valBuffer.Write(valBytes)
            
            if tokentype == "json.Delim"{
                iterateBracketCount(token, brack, curly)
            }
            
            if tokentype != "json.Delim" || token == brack.close || token == curly.close {   
                
                if brack.cntOpen == brack.cntClose && curly.cntOpen == curly.cntClose {
                    //  grab value and length of value from valBuffer
                    valLen = uint64(valBuffer.Len())
                    valLenBytes := make([]byte, 8)
                    binary.LittleEndian.PutUint64(valLenBytes, valLen)

                    valBytes = valBuffer.Bytes()
                    keyBytes := keyBuffer.Bytes() // contains keyLen and keyBytes from the else clause in if !key

                    m := append(append(append([]byte{}, keyBytes...), valLenBytes...), valBytes...)

                    //  stream [keyBuffer contents, valueLength, value] into channel
                    wg.Add(1)
                    writeChannel <- m

                    // book keeping
                    key = true
                    brack.resetOpen()
                    brack.resetClose()
                    curly.resetOpen()
                    curly.resetClose()
                    keyBuffer.Reset()
                    valBuffer.Reset()
                } else {
                    // add delimiter "," to valBuffer
                    valBuffer.Write([]byte(","))
                }

            }

        } else {
            if tokentype == "json.Delim"{
                continue
            }

            keyBytes := token.([]byte)
            keyLen = uint8(len(keyBytes))
            keyLenBytes := make([]byte, 1)
            // custom implementation of binary.LittleEndian.PutUint8() since it doesn't exist
            // https://go.dev/src/encoding/binary/binary.go
            _ = keyLenBytes[0] // this line is unnecessary but kep as is for duplicating other LittleEndian Put methods logic
            keyLenBytes[0] = byte(keyLen) // similar to above casting as byte unnecessary
            
            // stream keyLength and then keyBytes into keyBuffer
            keyBuffer.Write(keyLenBytes)
            keyBuffer.Write(keyBytes)
            key = false
        }

    }

    wg.Wait()

}



