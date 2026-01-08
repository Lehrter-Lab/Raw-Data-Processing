# Raw-Data-Processing

This repository contains all necessary scripts for processing outputs by lab & field instrumentation. All higher functions should be contained with the repository of the project they were written for.

## Preprocessor Script
Python alternative for Shimadzu TOC-V script with additional functionality for all other water quality analytes.

**IMPORTANT**: Samples should be labeled in the 'Sample Name' column. Quality controls and drift checks should have 'QC' in the 'Sample Name' column and an identifier in the 'Sample ID' column. Valid identifiers are 'Check', 'Spike', or the numeric concentration in PPM (e.g. 20).

## Olivia-Bot
This script grabs preprocessor script output for TNDOC and compares it to the master sheet (you need to download this yourself). It then updates the sheet with any new values from the preprocessor output and returns it as a new spreadsheet.

## Shimadzu TOC-V scripts
ShimadzuTOC-V is deprecated. DICTNDOC should be used instead for TOC-V data.

**IMPORTANT**: Samples should be labeled in the 'Sample Name' column. Quality controls and drift checks should have 'QC' in the 'Sample Name' column and an identifier in the 'Sample ID' column. Valid identifiers are 'Check', 'Spike', or the numeric concentration in PPM (e.g. 20).

If you are using the auto mode, ensure that your folder only has files that you want the script to iterate over and that the absolute path is correctly entered.

The threshold for a standard flags in the script is an ordinary r-squared of < 0.9990. This flag is conservative and should be adjusted upwards as necessary.

If standards are excluded in the TOC software, that will not be reflected in the script. Make a note of any such exclusions.
