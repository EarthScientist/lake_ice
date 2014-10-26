# Western Alaska Lake Mapping

### Description:
Some scripts related to the data wrangling and analysis related to the Western Alaska Lake Mapping effort.


#### Current Scripts:

###### lake_checker_serial.py - A very primitive set of functions for removing overlapping lake polygons due to the 
						object oriented image detection input images having overlapping tiles.  The overall 
						goal here was to locate the offending polygons from sometimes several overlapping 
						layers and remove the ones in the file which has the oldest date.  In cases where 
						multiple tiles overlapped on the same image collection date, a simple method of using 
						the tile number of each of the tiles to decide which polygon was kept.

###### convert_to_geojson.py - A small script used to convert the shapefiles generated wth lake_checker or 
						lake_checker_serial into geojson for further work, mostly web related.


