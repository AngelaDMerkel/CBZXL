# CBZXL

This Python script unpacks CBZ archives and converts .JPEG and .PNG files to JXL using lossless compression at a user configurable effort level. The script includes basic logging and error handling. 

## Features

- Converts JPEG and PNG to JXL losslessly
- Renames incorrect extensions using MIME type
- Corrects atypical or unusual colour space or metadata
- SQLite to skip reviously processed archives
- Basic logging

## Caveats

The script is pretty unsophisticated and it's up to the user to have `magic`, `cjxl` etc in their path for the script to use. As far as I can tell, it catches every edge case you're likely to encounter. 
