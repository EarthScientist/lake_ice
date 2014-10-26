'''*********************************************************************
Requirement: ArcGIS 3D Analyst extension
**********************************************************************'''

# Import system modules
import arcpy
from arcpy import env

# Obtain a license for the ArcGIS 3D Analyst extension
arcpy.CheckOutExtension("3D")

# Set environment settings
env.workspace = raw_input('Enter the name of the workspace folder: ')
#output_SHAPE = raw_input('Enter the name of the output shapefile with extension .shp: ')


#Creating footprint from the image file
try:
    # Create the list of rasters
    rasterList = arcpy.ListRasters("*", "TIF")
    for raster in rasterList:
        print raster
      
    # Verify there are rasters in the list
    if rasterList:
        # Loop the process for each raster
        for raster in rasterList:
            # Set Local Variables
            outGeom = "POLYGON" # output geometry type
            # The [:-4] strips the .img from the raster name
            outPoly = raster[:-4] + "_FPrint" + ".shp"
            print "Creating footprint polygon for " + raster + "."
            #Execute RasterDomain
            arcpy.RasterDomain_3d(raster, outPoly, outGeom)
        print "Footprint creation complete."
    else:
        "There are no TIF files in the " + env.workspace + " directory."
       
except Exception as e:
    # Returns any other error messages
    print e.message
# ----------------------------------------------
# Check if the Raster footprint intersect and if they do check repeated water bodies in the overlapping area
# If Water Bodies are repeated then copy the Water Body name from the old date to a new table 'TableClip_OldDate * .dbf' which again will be merged into a big table later. Later this table will be used to delete all the repeated water bodies. We want recent water bodies.
try:    
    # Create the list of footprint shapefiles 
    footPrintList = arcpy.ListFeatureClasses("*_FPrint.shp", "Polygon")
    print footPrintList
    fP_Number_1 = 0
    
    # Verify there are polygons in the list
    if footPrintList:
        # Loop the process for each footprint
        for footPrint_1 in footPrintList:
            # If it is last in the list then the process is complete    
            if footPrint_1 == footPrintList[len(footPrintList)-1]:
                exit
                print "Last in the list - Done!"
            # If not last in the list then compare overlap and repeated Water Bodies
            else:
                print "stage 1: Begin - Grabbing a new tile"
                counter = len(footPrintList) - fP_Number_1
                #print str(counter)
                fP_Number_2 = fP_Number_1 + 1
               
                #Grab the first shapefile
                lakeArea_1 = footPrint_1[:-11] +'_LWB.shp'
                print "Grabbing the first shapefile: " + lakeArea_1
                #Grab Date of the first tile
                date_1 = int(footPrint_1[9:-19])
                print "Date of first shapefile: " + str(date_1)
       

                # Grab the second tile
                i = 1
                while i < counter:
                    count_2fP = i
                    
                    print "stage 2: Checking if two tiles overlap"    
                    footPrint_2 = footPrintList[fP_Number_2]
                    
                    # Compare if two tiles, footPrint_1 and footPrint_2 overlap
                    if arcpy.Exists('footPrintClip.shp'):
                        arcpy.Delete_management('footPrintClip.shp')
                    try:
                        footPrintClip = 'footPrintClip.shp'
                    except:
                        arcpy.AddError(arcpy.GetMessages(2))
                        
                    #overlap = 0 # 0 means NO overlap between tiles
                    arcpy.Clip_analysis(footPrint_1, footPrint_2, footPrintClip)
                    count = str(arcpy.GetCount_management(footPrintClip[:-4] + '.dbf'))
                    count_fP = int(count)
                    # Two tiles are overlapping
                    if count_fP >= 1:
                        #print str(count_fP)
                        #overlap = 1 # 1 means YES to overlap between tiles
                        print footPrint_1[:-11] + " overlaps " + footPrint_2[:-11]

                        #Grab the second shapefile to check what water bodies are repeated
                        lakeArea_2 = footPrint_2[:-11] +'_LWB.shp'
                        print "Grabbing the second shapefile: " + lakeArea_2
                        #Grab Date of the second tile                   
                        date_2 = int(footPrint_2[9:-19])
                        print "Date of second shapefile " + str(date_2)

                        #-------------------------------------------------------------------
                        #Clip the lakeArea_1, we are only interested in clip area, save time
                        if arcpy.Exists('lakeAreaClip_1.shp'):
                            arcpy.Delete_management('lakeAreaClip_1.shp')
                        try:
                            lakeArea_1_C = 'lakeAreaClip_1.shp'
                        except:
                            arcpy.AddError(arcpy.GetMessages(2))
                        print "Clipping " + lakeArea_1
                        arcpy.Clip_analysis(lakeArea_1, footPrintClip, lakeArea_1_C)    

                        #Clip the lakeArea_2, we are only interested in clip area, save time
                        if arcpy.Exists('lakeAreaClip_2.shp'):
                            arcpy.Delete_management('lakeAreaClip_2.shp')
                        try:
                            lakeArea_2_C = 'lakeAreaClip_2.shp'
                        except:
                            arcpy.AddError(arcpy.GetMessages(2))
                        print "Clipping " + lakeArea_2
                        arcpy.Clip_analysis(lakeArea_2, footPrintClip, lakeArea_2_C)    
                        #-------------------------------------------------------------------
                        
                        # Water Bodies Overlap/Reapeat Comparision Starts Here
                        if arcpy.Exists(lakeArea_1[:-16] + '_C_lyr'):
                            arcpy.Delete_management(lakeArea_1[:-16] + '_C_lyr')
                        try: 
                            lakeArea_Layer_1 = arcpy.MakeFeatureLayer_management(lakeArea_1_C, lakeArea_1[:-16] + '_C_lyr')
                            #print lakeArea_Layer_1
                        except:
                            arcpy.AddError(arcpy.GetMessages(2))
                                                 
                        #Check if there are overlapping repeated water bodies
                        rows_lakeArea_1 = arcpy.SearchCursor(lakeArea_1)
                        print "stage 3: Checking Water Bodies"
                        for row_lakeArea_1 in rows_lakeArea_1:
                            #print "stage 3: Checking Water Bodies"
                            idd = str(row_lakeArea_1.FID)
                            arcpy.SelectLayerByAttribute_management(lakeArea_Layer_1, "NEW_SELECTION", "FID = " + idd)
                            
                            # Run Clip Analysis to compare if water bodies polygons on lakeArea_1 and lakeArea_2 overlap
                            #i.e. they are from the same locations
                            if arcpy.Exists('clip.shp'):
                                arcpy.Delete_management('clip.shp')
                            try:
                                ClipFile = 'clip'
                            except:
                                arcpy.AddError(arcpy.GetMessages(2))
                                
                            #lakeArea_2 is the file to the be clipped i.e. input feature so ClipFile will store the lakeArea_2 feature that is clipped by lakeArea_Layer_1   
                            arcpy.Clip_analysis(lakeArea_2_C, lakeArea_Layer_1, ClipFile)
                            count = str(arcpy.GetCount_management(ClipFile + '.dbf'))
                            count_ClipWB = int(count)
                            if count_ClipWB >= 1:
                                # A Table to store polygons from old dates, later this will be compared with union shapefile and removed
                                TableClip_OldDate = 'Table_OldDate_' + str(fP_Number_1) + str(fP_Number_2) + '_' + idd + '.dbf'
                                #print TableClip_OldDate
                                #Compare dates: Grabs Year and Julian date from the file name
                                if date_1 < date_2:
                                    #print "Error"
                                    arcpy.CopyRows_management(lakeArea_Layer_1, TableClip_OldDate)
                                else:
                                    arcpy.CopyRows_management(ClipFile + '.dbf', TableClip_OldDate)
                    else:
                        print footPrint_1[:-11] + " DO NOT overlap " + footPrint_2[:-11]

                    print "Checking water bodies done. Moving on to the next footprint to check repeated water body."
                    #Move on to next footPrint_2
                    i = i + 1
                    fP_Number_2 = fP_Number_2 + 1

                                
            #Before moving on to another loop, i.e. to next footPrint_1, merge all the tables that was created in this loop and delete those individudal tables.
            #Tables are only crated when there is overlap between tiles
            listTable = arcpy.ListTables('Table_*', 'dBASE')
            #Check if listTable is empty
            if listTable: 
                resultTable = 'Final_OldDate_LWB_' + str(fP_Number_1) + '.dbf'
                print "Merging all the tables in one table table: " + resultTable
                #Delete all individual intermediate table results
                n = 0
                for table in listTable:
                    if n == 0:
                        arcpy.Merge_management(table, resultTable)
                        n = n+1
                    else:
                        n=n+1
                        arcpy.Append_management(table, resultTable)
                    #delete individual result table   
                    arcpy.Delete_management(table)
            # Move to next footPrint_1
            fP_Number_1 = fP_Number_1 + 1;
            overlap = ''


except Exception as e:
    # Returns any other error messages
    print e.message

print "DONE!"
