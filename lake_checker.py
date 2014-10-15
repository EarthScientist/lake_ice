#!/usr/local/Python2.7.6/bin/python
'''
LAKE_MAPPING - LCC - helper script
script to remove lakes being double counted on different image tiles at the overlap.
 the idea is in the case of overlap between any 2 layers (all combos) remove the offending
	polygons in the 'oldest' (or if same age choose first) layer.  These are all concatenated
	to a single large shapefile to be used in later analyses.
'''

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

def compare_dates( x ):
	'''
	take in 2 shapefile names and choose which is most_recent
	return a tuple in order of newest, oldest shapefile based 
	on embedded dates in filenames.

	arguments:
		x = 2-element tuple of string filenames
	'''
	fn1, fn2 = [ os.path.basename( i ).split('_')[0][9:-5] for i in x ]
	most_recent = str( max( int( fn1 ), int( fn2 ) ) )
	if most_recent in fn1:
		fn1, fn2 = x
		out = ( fn1, fn2 )
	else:
		fn1, fn2 = x
		out = ( fn2, fn1 )
	return out

def run( x ):
	'''
	a wrapper around all of the processing
	arguments:
		x = tuple of 2 shape filenames to be compared
	'''
	# unpack the filenames where shp1 is always the most recent file of the pair
	try:
		shp1_name, shp2_name = compare_dates( x )
		# print 'input filenames %s \n %s ' % x
		# open with fiona
		shp1 = fiona.open( shp1_name )
		shp2 = fiona.open( shp2_name )

		# calculate the intersection of the bboxes
		bounds_intersect = bbox_intersection( shp1, shp2 )

		# set a spatial filter over the x2 input polygons
		#	 shapefile iterator over a new fiona object with the 
		#	 bounding box of the intersecting domain
		shp1_spatfilt = shp1.filter( bounds_intersect.bounds )
		shp2_spatfilt = shp2.filter( bounds_intersect.bounds )

		shp1_pols = [ shape(pol['geometry']) for pol in shp1_spatfilt ]
		shp2_pols = [ shape(pol['geometry']) for pol in shp2_spatfilt ]

		shp1_pols_areas = [ shp1.area for shp1 in shp1_pols ]
		shp2_pols_areas = [ shp2.area for shp2 in shp2_pols ]

		# remove the large 'sea' polygon
		shp1_pols = [ i for i in shp1_pols if i.area < max( shp1_pols_areas ) ]
		shp2_pols = [ i for i in shp2_pols if i.area < max( shp2_pols_areas ) ]

		# return the offending polygons
		def test_intersect( shp1_cur, shp2_pols ):
			'''
			anonymous function to be passed to a parallel map command.
			returns all polygons (from shp2_pols) intersected between 2 polygon lists		
			'''
			return [ shp2 for shp2 in shp2_pols if shp1_cur.intersects( shp2 ) ]

		# run in multicore and close the pool.
		pool = mp.Pool( 16 )
		intersect_output = pool.map( lambda x: test_intersect( x, shp2_pols=shp2_pols ), shp1_pols )
		pool.close()

		# flatten the list & remove the None's
		intersect_output2 = [ j for i in intersect_output for j in i if j is not None ]

		# reopen the full shapefiles & convert to shapely objects
		shp1_pols = [ (pol, shape(pol['geometry']) ) for pol in fiona.open( shp1_name ) ]
		shp2_pols = [ (pol, shape(pol['geometry']) ) for pol in fiona.open( shp2_name ) ]
		
		# remove the large out-of-bounds polygon
		max_val1 = max( [ j.area for i,j in shp1_pols ] ) - 1000
		max_val2 = max( [ j.area for i,j in shp2_pols ] ) - 1000
		shp1_pols = [ i for i in shp1_pols if i[1].area < max_val1 ]
		shp2_pols = [ i for i in shp2_pols if i[1].area < max_val2 ]

		# remove the offenders from the larger shp2 polygon list (not spatfilter)
		intersect_output2 = list( set( intersect_output2 ) ) # use set to get uniques
		output_keepers_test = [ shp2_pols.pop( count ) for count, i in enumerate( shp2_pols ) for j in intersect_output2 if i[1].equals_exact( j, 0 ) ] 

		# append to all polygons in the full shp1 polygon list (not spatfilter)
		combined_keepers = shp1_pols
		[ combined_keepers.append( i ) for i in shp2_pols ] # combine the lists
		return combined_keepers
	except Exception as e:
		log_file.write( e )
		log_file.write( "- - - - - - - - END  - - - - - - - - - " )
		print( e )
		pass


if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	import pathos.multiprocessing as mp
	# import multiprocessing as mp # use if no pathos, but may not work...
	from shapely.geometry import *
	from itertools import combinations
	import numpy as np

	# some setup
	base_dir = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Landsat_OLI/Large_Water_Bodies_Only'
	output_filename = os.path.join( base_dir, 'some_filename_all_appended.shp' )
	polys = glob.glob( os.path.join( base_dir, '*.shp' ) )
	log_file = open( os.path.join( base_dir, 'CURRENT_LOG_FILE.txt' ), mode='w' )

	# run it 
	all_combinations = [ i for i in combinations( polys, 2 ) ] # all unique combos
	final_intersected = map( lambda x: run( x ), all_combinations )

	final_intersected_flat = final_intersected[0]
	final_intersected_flat = [ i for i in final_intersected_flat if i is not None ]
	final_intersected_flat = list( set( final_intersected_flat ) )

	# write out as a shapefile
	schema = fiona.open( polys[0] ).schema # open a template file for the schema
	with fiona.open( output_filename, 'w', 'ESRI Shapefile', schema) as c:
		for geom, shp in final_intersected_flat:
			c.write( geom )

	print( 'final output shapefile path: ' + output_filename )

