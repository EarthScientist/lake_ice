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

def test_and_remove( pols_list, test_pol ):
	'''
	pols_list should be the list of the 'older' date polygons or
	if not older than the first shp (shp1).

	test pol should be a single polygon to test.
	'''
	return [ pol for pol in pols_list if not pol.intersects( test_pol ) ]


def bbox_difference( shp1, shp2 ):
	'''
	simple function to return the difference of the bboxes of the
	2 fiona shapefiles extent bounding boxes. Used to filter the 
	shapefiles later.

	arguments:
		shp1 = fiona vector object
		shp2 = fiona vector object

	depends:
		fiona, shapely

	returns:
		a tuple containing ( shp1 difference bbox, and shp2 difference bbox )
	'''
	# calculate the first bbox and convert to a shapely shape
	minx, miny, maxx, maxy = shp1.bounds
	test_bounds = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	# calculate the second bbox and convert to a shapely shape
	minx, miny, maxx, maxy = shp2.bounds
	test_bounds2 = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	# get the intersection
	test_intersected = test_bounds.intersection( test_bounds2 )
	# return the difference for each
	return ( test_bounds.difference( test_intersected ), test_bounds2.difference( test_intersected ) )


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

def compare_extents( shp1, shp2 ):
	minx, miny, maxx, maxy = shp1.bounds
	test_bounds = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	minx, miny, maxx, maxy = shp2.bounds
	test_bounds2 = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	return test_bounds.intersects( test_bounds2 )

def split_intersected_bbox( shp, intersected_bbox ):
	not_intersected = shp.difference( intersected_bbox )
	are_intersected = shp.intersects( intersected_bbox )
	return are_intersected, not_intersected

def run( x ):
	'''
	a wrapper around all of the processing
	arguments:
		x = tuple of 2 shape filenames to be compared
	'''
	# unpack the filenames where shp1 is always the most recent file of the pair
	try:
		shp1_base, shp2_base = x 
		# print 'input filenames %s \n %s ' % x

		# calculate the intersection and anti-intersection of the bboxes
		bounds_intersect = bbox_intersection( shp1_base, shp2_base )
		# bounds_difference = bbox_difference( shp1, shp2 )

		# set a spatial filter over the x2 input polygons
		#	 shapefile iterator over a new fiona object with the 
		#	 bounding box of the intersecting domain
		shp1_spatfilt = shp1_base.filter( bounds_intersect.bounds )
		shp2_spatfilt = shp2_base.filter( bounds_intersect.bounds )

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
			returns all polygons that DO NOT intersect with the shp2_pols
			'''
			return [ shp2 for shp2 in shp2_pols if not shp1_cur.intersects( shp2 ) ]

		# run in multicore and close the pool.
		pool = mp.Pool( 16 )
		intersect_output = pool.map( lambda x: test_intersect( x, shp2_pols=shp2_pols ), shp1_pols )
		pool.close()

		# flatten the list & remove the None's
		intersect_output2 = [ j for i in intersect_output for j in i if j is not None ]

		# reopen the full shapefiles & convert to shapely objects
		shp1_pols = [ ( pol, shape(pol['geometry']) ) for pol in shp1_base ]
		shp2_pols = [ ( pol, shape(pol['geometry']) ) for pol in shp2_base ]
		
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
	except Exception, err:
		global err
		log_file.write( 'ERROR: %s\n' % str(err) )
		log_file.write( "- - - - - - - - END  - - - - - - - - - " )
		print( str(err) )
		pass


if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	import pathos.multiprocessing as mp
	# import multiprocessing as mp # use if no pathos, but may not work...
	from shapely.geometry import *
	from itertools import combinations
	import numpy as np

	# some setup
	base_dir = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Version2/Large_Water_Bodies' # '/Users/malindgren/Documents/repos/tmp/Large_Water_Bodies' 
	output_filename = os.path.join( base_dir, 'some_filename_all_appended.shp' )
	polys = glob.glob( os.path.join( base_dir, '*.shp' ) )
	log_file = open( os.path.join( base_dir, 'CURRENT_LOG_FILE_2.txt' ), mode='w' )

	# run it 
	all_combinations = [ i for i in combinations( polys, 2 ) ] # all unique combos
	all_combinations = [ compare_dates((i,j)) for i,j in all_combinations if compare_extents(fiona.open(i), fiona.open(j)) ] # all uniques whose BBOX overlaps

	# maybe its best to just open the file handles? since there are some flushing and closing issues otherwise
	all_combinations_opened = [ ( fiona.open( i ), fiona.open( j ) ) for i,j in all_combinations ]

	# only_combos = { pol:[ pol2 for pol2 in opened_shapes if pol is not pol2 and compare_extents( pol, pol2 ) ] for pol in opened_shapes }

	final_intersected = map( lambda x: run( x ), all_combinations_opened )

	# final_intersected_flat = final_intersected[0]
	final_intersected_flat = [ i for i in final_intersected if i is not None ] # remove [None]
	final_intersected_flat = [ j for i in final_intersected_flat for j in i ] 

	# lets return only the uniques
	# out = list( set( [ shape for geom, shape in final_intersected_flat ] ))

	# final_intersected_flat = list( set( final_intersected_flat ) )

	# # write out as a shapefile
	schema = fiona.open( polys[0] ).schema # open a template file for the schema
	with fiona.open( output_filename, 'w', 'ESRI Shapefile', schema) as c:
		for geom, shp in final_intersected_flat:
			# since prajnas files have no standard anything
			new = OrderedDict()
			{ new.update({key:geom['properties'][ key ]}) for key in geom['properties'] if key in schema['properties'].keys() }
			geom['properties'] = new
			c.write( geom )

	log_file.close()

	print( 'final output shapefile path: ' + output_filename )



# # new thoughts:
# 1. make dictionaries of the overlapping shapefiles bboxes with one shapefile as the key and the others as the values list
# 2. find the common region of overlap between all bboxes and compute a bbox around this region
# 3. potentially filter the files spatially using the intersect polygon into regions of overlap and regions of no overlap
# 	- the non intersecting regions can go into a single list of polygons.
# 	- the remainder need to be in nested lists where we know the date of the polygons we are testing

# 1. copy all files to be run into a folder where they can be modified in the loop




