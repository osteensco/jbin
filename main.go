package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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

func (b *bracket) addOpen() {
    b.cntOpen += 1
}

func (b *bracket) addClose() {
    b.cntClose += 1
}

func newBracket(open string, close string) bracket {
    return bracket {open, close, 0, 0}
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

    decoder := json.NewDecoder(file)
    
    brack := newBracket("[", "]")
    curly := newBracket("{", "}")
    key := true
    
    for more := true; more; {
        
        token, err := decoder.Token()
        if err == io.EOF {
            break
        }

        if err != nil {
            fmt.Println("Error decoding token: ", err)
        }
        
        more = decoder.More()
        
        if key {
            //something
        } else {
            
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



