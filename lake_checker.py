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

def compare_dates( x ):
	'''
	take in 2 shapefile names and choose which is most_recent
	return a tuple in order of newest, oldest shapefile based 
	on embedded dates in filenames.

	arguments:
		x = 2-element tuple of string filenames

	'''
	fn1, fn2 = [ os.path.basename( i ).split('_')[0].strip('LM')[:-5] for i in x ]
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
	a wrapper around all of the processing needs

	arguments:
		x = tuple of 2 shape filenames to be compared
	'''
	# unpack the filenames where shp1 is always the most recent file of the pair
	shp1_name, shp2_name = compare_dates( x )

	# open with fiona
	shp1 = fiona.open( shp1_name )
	shp2 = fiona.open( shp2_name )

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
	shp1_pols = [ i for i in shp1_pols if i.area < max( shp1_pols_areas ) ]
	shp2_pols = [ i for i in shp2_pols if i.area < max( shp2_pols_areas ) ]

	shape_generator = [ {shp1:shp2_pols} for shp1 in shp1_pols ]

	# return the offending polygons, then remove them
	def test_intersect( x ):
		cur_shp = x.keys()[0]
		shp2_pols = x.values()[0]
		shp2_pols_out = shp2_pols
		return [ shp2 for shp2 in shp2_pols if cur_shp.intersects( shp2 ) ]

	# run in multicore and close the pool.
	pool = mp.Pool( 30 )
	print 'multiprocessing now...' + str( len( shape_generator ) )
	intersect_output = pool.map( test_intersect, shape_generator )
	print 'closing pool...'
	pool.close()

	# flatten the list
	intersect_output2 = [ j for i in intersect_output for j in i ]
	# reopen inputs & convert to shapely objects
	shp1_pols = [ (pol, shape(pol['geometry']) ) for pol in fiona.open( shp1_name ) ]
	shp2_pols = [ (pol, shape(pol['geometry']) ) for pol in fiona.open( shp2_name ) ]

	print 'remove biggies'
	# if these are still a big problem try somthing like this:
		# areas1 = np.array([ j.area for i,j in shp1_pols ])
		# test = areas1.astype(str)
		# length_counts = Counter( map(len, test) ) # divide on the smallest one or two?
	def remove_biggie( x ):
		pol, max_val = x
		i,j = pol
		if j.area < max_val:
			return pol
	
	# remove in parallel from shp1
	pool = mp.Pool( 30 )
	max_val = max( [ j.area for i,j in shp1_pols ] ) - 1000
	input_generator = ( (i, max_val) for i in shp1_pols )
	shp1_pols = pool.map( lambda x: remove_biggie( x ), input_generator )
	pool.close()
	# remove in parallel from shp2
	pool = mp.Pool( 30 )
	max_val = max( [ j.area for i,j in shp2_pols ] ) - 1000
	input_generator = ( (i, max_val) for i in shp2_pols )
	shp2_pols = pool.map( lambda x: remove_biggie( x ), input_generator )
	pool.close()
	
	# [potential future bug] ...  figure this one out... 
	# 	for some reason there is a None in the output here:  this removes it... hackily
	shp2_pols = [ i for i in shp2_pols if isinstance(i, tuple) ]

	# remove biggies in serial.. keep for testing
	# shp1_pols = [ (i,j) for i,j in shp1_pols if j.area < max( shp1_pols ) ]
	# shp2_pols = [ (i,j) for i,j in shp2_pols if j.area < max( shp2_pols ) ]

	# remove the ones that overlapped from the 'old' list
	print 'remove it --  large bottleneck currently...'
	def remove_old_overlaps( intersected_pol, old_polygons_list ):
		return [ (i,j) for i,j in old_polygons_list if j.bounds != intersected_pol.bounds ]

	pool = mp.Pool( 30 )
	shp2_pols = pool.map( lambda x: remove_old_overlaps( x, shp2_pols ), intersect_output2 ) 
	pool.close()

	print 'append it'
	[ shp1_pols.append( i ) for i in shp2_pols ] 
	return shp1_pols


if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	import pathos.multiprocessing as mp
	# import multiprocessing as mp
	from shapely.geometry import *
	from itertools import combinations
	import numpy as np

	# some setup
	base_dir = '/workspace/UA/malindgren/projects/Prajna/Test_Files'
	output_filename = os.path.join( base_dir, 'some_filename_all_appended.shp' )

	# loop through the rasters in some chronological order
	tiffs = glob.glob( os.path.join( base_dir, '*.tif' ) )
	# polys = map( lambda x: x.replace( '.tif', '_WB.shp' ), tiffs )
	polys = glob.glob( os.path.join( base_dir, '*.shp' ) )
	polys = [polys[0], polys[2]] # temporary due to broken file...
	all_combinations = combinations( polys, 2 )

	# run it 
	final_intersected = map( lambda x: run( x ), all_combinations )
	final_intersected_flat = final_intersected[0]

	# # hacky way to solve an issue I couldnt figure out above...
	# # figure out why nested lists were being tossed in here too...
	just_lists = [ i for i in final_intersected_flat if isinstance( i, list) ]
	just_shapes = [ i for i in final_intersected_flat if not isinstance( i, list) ]

	hold = [ just_shapes.append(i) for i in just_lists[0] ] 

	# then we need to combine our outputs with the outputs from the above
	# 	potentially do this in a loop to store all members in the end and 
	#	write to shapefile.
	schema = fiona.open( polys[0] ).schema # open a template file for the schema
	with fiona.open( output_filename, 'w', 'ESRI Shapefile', schema) as c:
		for geom, shp in just_shapes:
			c.write( geom )

	print output_filename

