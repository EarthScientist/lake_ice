import fiona, json, glob, os, sys

input_path = '/workspace/UA/malindgren/projects/Prajna/Data/Input/Version2/Large_Water_Bodies/removed_overlap'
output_path = '/workspace/UA/malindgren/projects/Prajna/CODE/json'

if not os.path.exists( output_path ):
	os.mkdir( output_path )

files = glob.glob( os.path.join( input_path, '*.shp' ) )

features = []
crs = None

for f in files:
	with fiona.collection( f, "r" ) as source:
		for feat in source:
			# feat['properties'].update(...) # with your attributes [ML: we aint got none...]
			features.append( feat )
		if crs == None: # lets only do this once.
			crs = " ".join( "+%s=%s" % (k,v) for k,v in source.crs.items() )

my_layer = {
	"type": "FeatureCollection",
	"features": features,
	"crs": {
		"type": "link", 
		"properties": { "href": "WesternAlaska_Lakes.crs", "type": "proj4" } }}

# write the geojson file
with open( os.path.join( output_path, "WesternAlaska_Lakes.json" ), "w" ) as f:
	f.write( json.dumps( my_layer ) )

# write the crs file
with open( os.path.join( output_path, "WesternAlaska_Lakes.crs" ), "w" ) as f:
	f.write( crs )