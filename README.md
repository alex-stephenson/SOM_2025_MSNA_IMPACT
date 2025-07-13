## About The Project

The Multi Sectoral Needs Assessment (MSNA) is IMPACT REACH's flagship research cycle. 

## Using this Repo

The code requires several inputs:
1. The tool
2. Raw kobo data
3. Geospatial sample data
4. Logical checks input
5. LoA for analysis
6. FO mapping data

Optional inputs include:
1. Deletions that should be removed from the deletion list
2. Surveys that are validated as non-soft duplicate.

The code has several outputs:

1. The cleaning and deletion logs
2. The clean and raw data
3. The results tables, both in the standard REACH format and as required for the IPC

## Dependencies

The script requires you to download two packages from GitHub: `ImpactFunctions`, which has been developed by Alex Stephenson to create re-usable, modularised code, and `Keyring`for securely calling passwords within scripts. You set a password using `keyring::key_set("Test_Service", "My_Username")`, which will prompt an entry box for your password. Enter the password and then when you call `keyring::key_get("Test_Service", "My_Username")` it will return your password. This can be done in the command line. 
:
## Contact

Please contact alex.stephenson@impact-initiatives.org for any queries. Or fork the repo and make a pull request. 
