# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# THIS IS THE NEW WORKING VERSION THOUGH SLOW AS MOLASSES
#  not parallelized version.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def return_overlapping( shp_list ):
	return { pol:[ pol2 for pol2 in shp_list 
			if pol is not pol2 and compare_extents( pol, pol2 ) ] 
				for pol in shp_list }

if __name__ == '__main__':
	import rasterio, fiona, shapely, glob, os, dill
	import pathos.multiprocessing as mp
	# import multiprocessing as mp # use if no pathos, but may not work...
	from shapely.geometry import *
	from itertools import combinations
	import numpy as np

	# some setup
	base_dir = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Version2/Large_Water_Bodies' # '/Users/malindgren/Documents/repos/tmp/Large_Water_Bodies' 
	output_filename = os.path.join( base_dir, 'Lakes_Merged_v0_1_akalbers.shp' )
	# polys = glob.glob( os.path.join( base_dir, '*.shp' ) )
	log_file = open( os.path.join( base_dir, 'CURRENT_LOG_FILE.txt' ), mode='w' )

	# get em all in the same ref sys in this case I am using AK ALBERS -- one time run
	# for poly in polys:
	# 	os.system( 'ogr2ogr -t_srs EPSG:4326 ' + poly.replace( '.shp', '_akalbers.shp' ) + ' ' + poly )

	polys = glob.glob( os.path.join( base_dir, '*_akalbers.shp' ) )

	# kinda maybe working? YES! takes 7.5 hours though
	all_overlap_dict = return_overlapping([fiona.open(i) for i in polys ] )

	final=defaultdict()
	for key in all_overlap_dict:
		print key.name
		out = []
		intersect_list = all_overlap_dict[ key ]
		base_shape = key
		base_shape_pols = [ shape(i['geometry']) for i in base_shape ]
		for i in intersect_list:
			print '  ' + i.name
			i_pols = [ shape(j['geometry']) for j in i ]
			if int(i.name.split('_')[0][9:-5]) < int(base_shape.name.split('_')[0][9:-5]): 
				for pol1 in i_pols:
					for index, pol2 in enumerate( base_shape_pols ):
						if pol2.intersects( pol1 ):
							out.append( index )
			# how to properly choose one if their dates match? I am taking the smaller initial val of the 2 no reason for it
			elif int(i.name.split('_')[0][2:9]) < int(base_shape.name.split('_')[0][2:9]):
				for pol1 in i_pols:
					for index, pol2 in enumerate( base_shape_pols ):
						if pol2.intersects( pol1 ):
							out.append( index )
		final[ key ] = out


	# output final as JSON for work later
	to_json = { i.name:final[i] for i in final.keys() }
	output_json = os.path.join( base_dir, 'output_offenders_indexes.json' )
	with open( output_json, 'w' ) as outfile:
		json.dump( to_json, outfile )

	# get the unique keys from the dict
	for key in final.keys():
		poly_list = [ pol for pol in key ]
		ind = final[ key ]
		keep_poly_list = [ pol for index, pol in enumerate( poly_list ) if index not in ind ]

		# write the data to a shapefile
		output_path = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Version2/Large_Water_Bodies/removed_overlap/'
		output_fn = os.path.join( output_path, key.name + '_remove_overlapping_polys.shp' )
		schema = key.schema.copy()
		with fiona.open( output_fn, 'w', 'ESRI Shapefile', schema ) as shp:
			for geom in keep_poly_list:
				shp.write( geom )
			print 'completed: '+ output_fn


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
