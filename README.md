# CBZXL

This bash script unpacks CBZ archives and converts .JPEG and .PNG files to JXL using lossless compression at a user configurable effort level. The script includes basic logging and error handling. 

## Features

- Converts JPEG and PNG to JXL losslessly
- Ignores file types `cjxl` cannot handle
- Renames incorrect extensions using MIME type
- Skips previously converted archives

## Caveats

This script was written for macOS which uses an older `bash`version and doesn't support some of the utilities that linux supports. In theory it ought to work fine on any platform, but I can't guarantee that. 
