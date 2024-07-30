Converts json file to .bin.
Uses a simple custom serialization format, mostly for the purpose of converting json files containing only string key-value pairs.

Length of keys must fit in 1 byte, length of values must fit in 2 bytes.
Keys and values will always be serialized as strings for simplicity.

Serialization format is as follows:
```[len(key) (1 byte)][key (n bytes)][len(value) (2 bytes)][value (i bytes)]```
