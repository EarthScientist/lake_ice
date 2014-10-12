# # # # #
# a simple script to make sure that we are not double counting lakes in overlap regions
# # # # # 
if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	# import pathos.multiprocessing as mp
	import multiprocessing as mp
	from shapely.geometry import *

	base_dir = '/workspace/UA/malindgren/projects/Prajna/Test_Files'

	# loop through the rasters in some chronological order
	tiffs = glob.glob( os.path.join( base_dir, '*.tif' ) )
	polys = map( lambda x: x.replace( '.tif', '_WB.shp' ), tiffs )
	# tiff_shp = zip( tiffs, polys )
	# combinations = [i for i in combinations(map(os.path.basename, tiffs), 2)]

	# convert bounds to polygon x2
	shp = fiona.open( polys[0] )
	minx, miny, maxx, maxy = shp.bounds
	test_bounds = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})
	
	shp2 = fiona.open( polys[1] )
	minx, miny, maxx, maxy = shp2.bounds
	test_bounds2 = shape({'coordinates':[[(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)]], 'type':'Polygon'})

	# intersection of the two bounds 
	bounds_intersect = test_bounds.intersection( test_bounds2 )

	# set a spatial filter over the x2 input polygons
	# 	 shapefile iterator over a new fiona object with the 
	#	 bounding box of the intersecting domain
	shp_spatfilt = shp.filter( bounds_intersect.bounds )
	shp2_spatfilt = shp2.filter( bounds_intersect.bounds )

	shp_pols = [ shape(pol['geometry']) for pol in shp_spatfilt ]
	shp2_pols = [ shape(pol['geometry']) for pol in shp2_spatfilt ]

	shp_pols_areas = [ shp.area for shp in shp_pols ]
	shp2_pols_areas = [ shp2.area for shp2 in shp2_pols ]

	# remove the large 'sea' polygon
	shp2_pols = [ i for i in shp2_pols if i.area < max(shp2_pols_areas) ] 
	shp_pols = [ i for i in shp_pols if i.area < max(shp_pols_areas) ] 

	shape_generator = [ {shp:shp2_pols} for shp in shp_pols ]

	def test_intersect( x ):
		cur_shp = x.keys()[0]
		shp2_pols = x.values()[0]
		return { cur_shp:[ cur_shp.intersects( shp2 ) for shp2 in shp2_pols ] }

	pool = mp.Pool( 20 )

	print 'multiprocessing now...' + str(len( shape_generator ))
	intersect_output = pool.map( test_intersect, shape_generator )

	print 'closing pool...'
	pool.close()

	# a little test for overlap:
	# [[True for i in j.values()[0] if i == True ] for j in intersect_output]

	#	 roll that into a shapely geometry for intersection-y stuff
	# shp_multi = MultiPolygon([shape(pol['geometry'])) for pol in shp_spatfilt ])
	# shp2_multi = MultiPolygon([shape(pol['geometry']) for pol in shp2_spatfilt ])
	# %time big_test = shp2_multi.intersection( shp_multi ) # 28 mins...


	# operate only over those filtered areas (spatially)


	# intersect_test = shp2_multi.intersects( shp_multi ) # not sure if this is doable or not

	# calculate some intersects between the polygons therein. 
	# --> would be dope to know how much overlap there is... so that we can decide to drop based on ovelap.

	# if there is intersection take the newer one. 

	# return as a list of the polygons we want
	# if the date is the same then just choose the first one in the list



	# shp = fiona.open( polys[0] )
	# poly_list = [shape(pol['geometry']) for pol in fiona.open(polys[0]) ]
	# test_intersect = [ pol2.intersects( pol ) for pol in poly_list for pol2 in poly_list if pol2 is not pol ]


