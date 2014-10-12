# # # # #
# a simple script to make sure that we are not double counting lakes in overlap regions
# # # # # 

def bbox_intersection( shp1, shp2 ):
	'''
	simple function to return the bounding box
	of the returned intersection of 2 fiona shapefile
	extent bounding boxes.

	arguments:
		shp1 = fiona vector object
		shp2 = fiona vector object

	depends:
		fiona, shapely
	'''
	# calculate the first bbox and convert to a shapely shape
	minx, miny, maxx, maxy = shp1.bounds
	test_bounds = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	# calculate the second bbox and convert to a shapely shape
	minx, miny, maxx, maxy = shp2.bounds
	test_bounds2 = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})

	return test_bounds.intersection( test_bounds2 )


def run( x ):
	'''
	a wrapper around all of the processing needs

	arguments:
		x = tuple of 2 shape filenames to be compared
	'''
	# unpack the filenames
	shp1, shp2 = x

	# open with fiona
	shp1 = fiona.open( shp1 )
	shp2 = fiona.open( shp2 )

	# calculate the intersection of the bboxes
	bounds_intersect = bbox_intersection( shp1, shp2 )

	# set a spatial filter over the x2 input polygons
	# 	 shapefile iterator over a new fiona object with the 
	#	 bounding box of the intersecting domain
	shp1_spatfilt = shp1.filter( bounds_intersect.bounds )
	shp2_spatfilt = shp2.filter( bounds_intersect.bounds )

	shp1_pols = [ shape(pol['geometry']) for pol in shp1_spatfilt ]
	shp2_pols = [ shape(pol['geometry']) for pol in shp2_spatfilt ]

	shp1_pols_areas = [ shp1.area for shp1 in shp1_pols ]
	shp2_pols_areas = [ shp2.area for shp2 in shp2_pols ]

	# remove the large 'sea' polygon
	shp2_pols = [ i for i in shp2_pols if i.area < max(shp2_pols_areas) ] 
	shp1_pols = [ i for i in shp1_pols if i.area < max(shp1_pols_areas) ] 

	shape_generator = [ {shp1:shp2_pols} for shp1 in shp1_pols ]

	def test_intersect( x ):
		cur_shp = x.keys()[0]
		shp2_pols = x.values()[0]
		return { cur_shp:[ cur_shp.intersects( shp2 ) for shp2 in shp2_pols ] }

	# run in multicore and close the pool.
	pool = mp.Pool( 15 )
	print 'multiprocessing now...' + str(len( shape_generator ))
	intersect_output = pool.map( test_intersect, shape_generator )
	print 'closing pool...'
	pool.close()
	return intersect_output


if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	import pathos.multiprocessing as mp
	# import multiprocessing as mp
	from shapely.geometry import *
	from itertools import combinations

	base_dir = '/workspace/UA/malindgren/projects/Prajna/Test_Files'

	# loop through the rasters in some chronological order
	tiffs = glob.glob( os.path.join( base_dir, '*.tif' ) )
	# polys = map( lambda x: x.replace( '.tif', '_WB.shp' ), tiffs )
	polys = glob.glob( os.path.join( base_dir, '*.shp' ) )
	polys = [polys[0], polys[2]] # temporary due to broken file...
	combinations = combinations( polys, 2 )

	# run it 
	final_intersected = map( lambda x: run( x ), combinations )

