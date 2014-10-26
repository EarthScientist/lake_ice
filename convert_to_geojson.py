import fiona, json, glob, os, sys
from collections import OrderedDict

input_path = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Version2/Large_Water_Bodies/removed_overlap'
output_path = '/workspace/UA/malindgren/projects/Prajna/CODE/json'

if not os.path.exists( output_path ):
	os.mkdir( output_path )

files = glob.glob( os.path.join( input_path, '*.shp' ) )
keep_attributes = [ 'WB_Name' ]

features = []
crs = None

for f in files:
	outname = os.path.basename( f ).strip( '.shp' )
	with fiona.collection( f, "r" ) as source:
		for feat in source:
			props = feat['properties'].copy()
			feat['properties'].clear() # clear it out
			keep_props = [ (i, props[ i ]) for i in keep_attributes ] # get the ones we want
			feat['properties'].update( keep_props ) # update with what we want
			features.append( feat )
			crs = " ".join( "+%s=%s" % (k,v) for k,v in source.crs.items() )

	my_layer = {
		"type": "FeatureCollection",
		"features": features,
		"crs": {
			"type": "link", 
			"properties": { "href": outname + ".crs" , "type": "proj4" } }}

	# write the geojson file
	with open( os.path.join( output_path, outname + ".json" ), "w" ) as f:
		f.write( json.dumps( my_layer ) )

	# write the crs file
	with open( os.path.join( output_path, outname + ".crs" ), "w" ) as f:
		f.write( crs )