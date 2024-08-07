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
	"sort"
	"strings"
	"sync"
)

/*
Takes output of os.Args ([]string), removes program name, verifies only one arg present, and returns arg.
*/
func parseCmd(args []string) (string, error) {
    
    cmd := args[1:]
    l := len(cmd)

    if l > 1 {
        return "", errors.New(fmt.Sprintf("Too many args provided %v, usage: jbin <filepath>", args))
    }
     
    return cmd[0], nil

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
   
    defer close(c)
    for m := range c {
        
        file.Write(m)
        wg.Done()

    }

}
    
type streamProps struct {
    decoder *json.Decoder
    writeChannel chan []byte
    brack *bracket
    curly *bracket
    key bool
    firstDelim bool
    keyLen *uint8
    valLen *uint16
    valBuffer *bytes.Buffer
    keyBuffer *bytes.Buffer
    wg *sync.WaitGroup
}

/*
Reads in a Json file and passes contents into key and value buffers. 
Once a key-value pair is completed, streams contents into a write channel for go routine to write to disk.
*/
func streamJson(prop *streamProps) {
    for {
       if prop.firstDelim {
            prop.firstDelim = false
            _, err := prop.decoder.Token()
            if err == io.EOF {
                break
            }
            continue
        }
        
        token, err := prop.decoder.Token()
        if err == io.EOF {
            break
        }

        if err != nil {
            fmt.Println("Error streaming json: ", err)
            os.Exit(1)
        }

        tokentype := fmt.Sprintf("%v", reflect.TypeOf(token))
        // if _, ok := token.(string); !ok {
        //     fmt.Printf("Error asserting token as string: %v is type %v", token, tokentype)
        //     os.Exit(1)
        // }

        if !prop.key {
 
            valBytes := []byte(token.(string))
            if _, err := prop.valBuffer.Write(valBytes); err != nil {
                fmt.Println("Error writing to valBuffer: ", err)
                os.Exit(1)
            }

            if tokentype == "json.Delim"{
                iterateBracketCount(token, prop.brack, prop.curly)
            }
            
            if tokentype != "json.Delim" || token == prop.brack.close || token == prop.curly.close {   
                
                if prop.brack.cntOpen == prop.brack.cntClose && prop.curly.cntOpen == prop.curly.cntClose {
                    //  grab value and length of value from valBuffer
                    *prop.valLen = uint16(prop.valBuffer.Len())
                    valLenBytes := make([]byte, 2)
                    binary.LittleEndian.PutUint16(valLenBytes, *prop.valLen)

                    valBytes = prop.valBuffer.Bytes()
                    keyBytes := prop.keyBuffer.Bytes() // contains keyLen and keyBytes from the else clause in if !key

                    m := append(append(append([]byte{}, keyBytes...), valLenBytes...), valBytes...)

                    //  stream [keyBuffer contents, valueLength, value] into channel
                    prop.wg.Add(1)
                    prop.writeChannel <- m

                    // book keeping
                    prop.key = true
                    prop.brack.resetOpen()
                    prop.brack.resetClose()
                    prop.curly.resetOpen()
                    prop.curly.resetClose()
                    prop.keyBuffer.Reset()
                    prop.valBuffer.Reset()
                } else {
                    // add delimiter "," to valBuffer
                    if _, err := prop.valBuffer.Write([]byte(",")); err != nil {
                        fmt.Println("Error writing to valBuffer: ", err)
                        os.Exit(1)
                    }
                }

            }

        } else {
            if tokentype == "json.Delim"{
                continue
            }
            keyBytes := []byte(token.(string))
            *prop.keyLen = uint8(len(keyBytes))
            keyLenBytes := make([]byte, 1)
            // custom implementation of binary.LittleEndian.PutUint8() since it doesn't exist
            // https://go.dev/src/encoding/binary/binary.go
            _ = keyLenBytes[0] // this line is unnecessary but kept as is for duplicating other LittleEndian Put methods logic
            keyLenBytes[0] = byte(*prop.keyLen) // similar to above, casting as byte unnecessary
            
            // stream keyLength and then keyBytes into keyBuffer
            if _, err := prop.keyBuffer.Write(keyLenBytes); err != nil {
                fmt.Println("Error writing to keyBuffer: ", err)
                os.Exit(1)
            }
            if _, err := prop.keyBuffer.Write(keyBytes); err != nil {
                fmt.Println("Error writing to valBuffer: ", err)
                os.Exit(1)
            }

            prop.key = false
        }

    }

    prop.wg.Wait()
}

func main() {

    var path string
    var file *os.File
    var newFile *os.File
    var err error

    if path, err = parseCmd(os.Args); err != nil {
        fmt.Println(err)
        os.Exit(1)
    } 
    
    if file, err = os.Open(path); err != nil {
        fmt.Println("Error opening file: ", err)
        os.Exit(1)
    }
    defer file.Close()

    if newFile, err = os.Create(strings.Trim(path, ".json")+".bin"); err != nil {
        fmt.Println("Error creating new file", err)
        os.Exit(1)
    }
    defer newFile.Close()

    decoder := json.NewDecoder(file)
    writeChannel := make(chan []byte) 
    brack := newBracket("[", "]")
    curly := newBracket("{", "}")
    // key := true
    // firstDelim := true
    valBuffer := new(bytes.Buffer)
    keyBuffer := new(bytes.Buffer)
    wg := new(sync.WaitGroup)
    var keyLen uint8
    var valLen uint16

    props := streamProps{
       decoder,
       writeChannel,
       brack,
       curly,
       true,// key
       true,// firstDelim
       &keyLen,
       &valLen,
       valBuffer,
       keyBuffer,
       wg,
   }
    
    go writeToDisk(writeChannel, newFile, wg)
    
    streamJson(&props)

    // mp := readMap(newFile)
    // printMap(mp)

}

func printMap(hashmap map[string]string) {

    fmt.Println("")

    keys := make([]string, 0, len(hashmap))
    for k := range hashmap {
        keys = append(keys, k)
    }

    sort.Strings(keys)

    for i := range keys {

        fmt.Printf("%v: %v\n", keys[i], hashmap[keys[i]])
                                                        
    }

    fmt.Println("")

}


func readMap(file *os.File) map[string]string {

    pathMap := make(map[string]string)

    _, err := file.Seek(0,0) 
    if err != nil {
        fmt.Println("Error seeking to beginning of file: ", err)
        os.Exit(1)
    }

    // key length integer should always fit in 8 bits
    var keyLen uint8
    // value length integer should always fit in 16 bits
    var valLen uint16

    // this should iterate until the end of the file
    for {
        // read length of key, use length to read in key
        err = binary.Read(file, binary.LittleEndian, &keyLen)
        // End Of File error should only ever happen here
        if err == io.EOF {
            break
        }
        if err != nil {
            fmt.Println("Error reading keyLen binary: ", err)
            os.Exit(1)
        }
        keyBytes := make([]byte, keyLen)
        _, err = file.Read(keyBytes)
        if err != nil {
            fmt.Println("Error reading in keyBytes: ", err)
        }

        // read length of value, use length to read in value
        err = binary.Read(file, binary.LittleEndian, &valLen)
        if err != nil {
            fmt.Println("Error reading valLen binary: ", err)
            os.Exit(1)
        }
        valBytes := make([]byte, valLen)
        _, err = file.Read(valBytes)
        if err != nil {
            fmt.Println("Error reading in valBytes: ", err)
        }

        fmt.Printf("%v: %v",string(keyBytes), string(valBytes))
        fmt.Println("")
        pathMap[string(keyBytes)] = string(valBytes)

    }

    return pathMap
}

