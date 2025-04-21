# CBZXL

This bash script unpacks CBZ archives and converts .JPEG and .PNG files to JXL using lossless compression at a user configurable effort level. The script includes basic logging and error handling. 

## Features

- Converts JPEG and PNG to JXL losslessly
- Ignores file types `cjxl` cannot handle
- Renames incorrect extensions using MIME type
- Skips previously converted archives
