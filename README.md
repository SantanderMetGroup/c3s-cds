# c3s-cds
This repository contains scripts to download, preprocess, standardize, and consolidate the catalogues available in the CDS.

## Contents

| Directory | Contents |
| :-------- | :------- |
|  [requests](https://github.com/SantanderMetGroup/c3s-cds/tree/main/requests) | Contains one CSV file per CDS catalogue, listing the requested variables, the target save directory, and whether the variable is raw or requires post-processing to be standardized.
|  [scripts](https://github.com/SantanderMetGroup/c3s-cds/tree/main/scripts) | 	Python scripts to download data from the CDS.
| [provenance](https://github.com/SantanderMetGroup/c3s-cds/tree/main/provenance) | Contains one JSON file per catalogue describing the provenance and definitions of each variable.
| [standardization](https://github.com/SantanderMetGroup/c3s-cds/tree/main/standardization) |  Python recipes to standardize the variables.
| [catalogues](https://github.com/SantanderMetGroup/c3s-cds/tree/main/catalogues) |  	CSV catalogues of datasets consolidated in Lustre or GPFS. The catalogues are updated through a nightly CI job.


