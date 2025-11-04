# c3s-cds
This repository contains scripts to download, preprocess, standardize, and consolidate the catalogues available in the CDS.
## Environment:
The environment is the one used in the c3s-atlas user tools: https://github.com/ecmwf-projects/c3s-atlas/blob/main/environment.yml
## Filename format:
Format of the files is "{var}\_{dataset}\_{date}.nc"
With date: 
- "{year}{month}" for big datasets like CERRA saved month by month (download is faster this way). 
- "{year}" for the other datasets that are saved year by year. 
## Contents

| Directory | Contents |
| :-------- | :------- |
|  [requests](https://github.com/SantanderMetGroup/c3s-cds/tree/main/requests) | Contains one CSV file per CDS catalogue, listing the requested variables, the target save directory, and whether the variable is raw or requires post-processing to be standardized.
|  [scripts](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts) | 	Python scripts to download data from the CDS.
| [provenance](https://github.com/SantanderMetGroup/c3s-cds/tree/main/provenance) | Contains one JSON file per catalogue describing the provenance and definitions of each variable.
| [standardization](https://github.com/SantanderMetGroup/c3s-cds/tree/main/standardization) |  Python recipes to standardize the variables.
| [derived](https://github.com/SantanderMetGroup/c3s-cds/tree/main/derived) |  Python recipes to calculate derived products from the variables.
| [interpolation](https://github.com/SantanderMetGroup/c3s-cds/tree/main/interpolation) |  Python recipes to interpolate the data using reference grids.
| [catalogues](https://github.com/SantanderMetGroup/c3s-cds/tree/main/catalogues) |  	CSV catalogues of datasets consolidated in Lustre or GPFS. The catalogues are updated through a nightly CI job.


