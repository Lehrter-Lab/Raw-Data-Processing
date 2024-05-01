# Raw-Data-Processing

This repository contains all necessary scripts for processing outputs by lab & field instrumentation. All higher functions should be contained with the repository of the project they were written for.

## Shimadzu TOC-V scripts
Shimadzu interpreter is deprecated. TNDOC should be used instead for TOC-V data.

Samples should be labeled in the 'Sample Name' column. Quality controls and drift checks should have 'QC' in the 'Sample Name' column and an identifier in the 'Sample ID' column.

If you are using the auto mode, ensure that your folder only has files that you want the script to iterate over and that the absolute path is correctly entered.

The threshold for a standard flags in the script is an ordinary r-squared of < 0.9990. This flag is conservative and should be adjusted upwards as necessary.

If standards are excluded in the TOC software, that will not be reflected in the script. Make a note of any such exclusions.
