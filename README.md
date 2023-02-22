# Utah-Water-Weather

This repository contains several data files, containing all USDA reservoir weather stations within the state of Utah. This includes all metadata about the weather stations, including locations and names. Another file includes all cities within the state of Utah, along with their geo position and county. Several uploaders are included to upload these data files to a Firebase Firestore database. These uploaders populate your database with needed metadata for usage by the scrapers.

Other scrapers are included which are meant to be run frequently, which scrape and upload water levels at each weather station, and summarize rain/snow across Utah counties. All files can be run using Python 3, and using the requirements.txt to ensure all libraries are installed.

