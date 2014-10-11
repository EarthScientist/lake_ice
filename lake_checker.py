# # # # #
# a simple script to make sure that we are not double counting lakes in overlap regions
# # # # # 

import rasterio, fiona, shapely, glob, os
from shapely.geometry import *

base_dir = '/workspace/UA/malindgren/projects/Prajna/Test_Files'

# loop through the rasters in some chronological order
tiffs = glob.glob( os.path.join( base_dir, '*.tif' ) )

polys = map( lambda x: x.replace( '.tif', '_WB.shp' ), tiffs )
tiff_shp = zip( tiffs, polys )

for tiff, shp in tiff_shp:

combinations = [i for i in combinations(map(os.path.basename, tiffs), 2)]



# convert bounds to polygon x2
shp = fiona.open( polys[0] )
minx, miny, maxx, maxy = shp.bounds
#	 polygon from bounding box of a shapefile:
test_bounds = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})

# intersection of the two bounds 
shp2 = fiona.open( polys[1] )
minx, miny, maxx, maxy = shp2.bounds
test_bounds2 = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
bounds_intersect = test_bounds.intersection(test_bounds)

# set a spatial filter over the x2 input polygons
# 	 shapefile iterator over a new fiona object with the 
#	 bounding box of the intersecting domain
shp_spatfilt = shp.filter( bounds_intersect.bounds )
shp2_spatfilt = shp2.filter( bounds_intersect.bounds )
#	 roll that into a shapely geometry for intersection-y stuff
shp_multi = MultiPolygon([shape(pol['geometry']) for pol in shp_spatfilt ])
shp2_multi = MultiPolygon([shape(pol['geometry']) for pol in shp2_spatfilt ])

# operate only over those filtered areas (spatially)


# calculate some intersects between the polygons therein. 
# --> would be dope to know how much overlap there is... so that we can decide to drop based on ovelap.

# if there is intersection take the newer one. 

# return as a list of the polygons we want
# if the date is the same then just choose the first one in the list





def compare_poly:

for i in combinations:



shp = fiona.open( polys[0] )
poly_list = [shape(pol['geometry']) for pol in fiona.open(polys[0]) ]
test_intersect = [ pol2.intersects( pol ) for pol in poly_list for pol2 in poly_list if pol2 is not pol ]



with fiona.open(polys[0]) as shp:


# TEST
for tiff in tiffs:
	for tiff2 in tiffs:
		if tiff2 is not tiff:



