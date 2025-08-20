# CBZXL

This Python script unpacks CBZ archives and converts .JPEG and .PNG files to JXL using lossless compression at a user configurable effort level. The script includes basic logging and error handling. 

## Features

- Converts JPEG and PNG to JXL losslessly
- (Optionally and Intelligently) converts images lossily
- Renames incorrect extensions using MIME type
- Corrects atypical or unusual colour spaces or metadata
- SQLite to skip reviously processed archives
- SQLite for data visualisation with companion script
- Basic logging
- Basic archive error handling
- User configurable timeout

## Usage

- `--effort` overrides the default effort level of `10`
- `--threads` overrides the default of `10` threads
- `--verbose` for more info including per archive savings and percent saved. I recommend you pass this flag
- `--quiet` supresses all console messages except for critical errors
- `--suppress-skipped` overrides `--verbose` and supresses any messages indicating when an archive has been skipped
- `--backup` creates backups of the CBZ archives
- `--dry-run`
- `--no-flatten` prevents the script from flattening internal archive structure
- `--no-convert` prevent conversion to JXL but allows all other functions (including flattening)
- `--stats` displays database statistics
- `--reprocess-failed`
- `--reset-db` will delete both databases and allow the script to reinspect each archive from scratch
- `--recheck-all` will force every archive to be rescanned without requiring the database to be deleted first
- `--delete-empty-archives` will delete all archives which do not contain image files, even if they contain other file types
- `--distance` will enable lossy conversion at the specified distance e.g. `0.1`
- `--smart-distance` triggers a lossy conversion only for images above 10MB or 5 megapixels and defaults to a distance of `0.1` or whatever distance has been specified in `--distance`

## Visualise.py

Can be run using `python visualise.py` and will look for a appropriately named .db file in the working directory and use that to print some statistics inside the terminal. Additionally, `--html-report MyConversionReport.html` can be appended to write an HTML file containing both the statistics normally output to the console alongside embedded graphs. 

## Caveats

The script is pretty unsophisticated and it's up to the user to have `magic`, `cjxl` etc in their path for the script to use. As far as I can tell, it catches every edge case you're likely to encounter. 
