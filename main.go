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
    var outerBracket bracket
    // valBuff - need buffer for values that are objects
    key := true
    firstDelim := true

    for more := true; more; {
        
        token, err := decoder.Token()
        if err == io.EOF {
            break
        }

        if err != nil {
            fmt.Println("Error decoding token: ", err)
        }
        
        more = decoder.More()
       
        // first token should always be an outer bracket
        // outerBracket will be used to check for end of object later
        if firstDelim {
            switch token {
            case "[": 
                outerBracket = newBracket("[", "]")
            case "{":
                outerBracket = newBracket("{", "}")
            }
            firstDelim = false
        }

        if !key {
            // check token type
            // if delim then add to brack/curly open/close count
            // add bytes to value buffer
            // if not delim or a close bracket, check open/close counts of brack and curly
            // if cntOpen == cntClose for both then 
            //      grab length of value in buffer
            //      stream valueLength and then value 
            //      switch key variable (key = true)
        } else {
            // convert key to bytes
            // take length of bytes
            //stream keyLength and then keyBytes
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



