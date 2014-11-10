#!/usr/local/bin/Rscript

##################################################################################################################
# Setup working directory


##################################################################################################################
# load library
library(httr)
library(rgdal)
library(reshape)
library(RSQLite)
library(sqldf)
library(leafletR)

Sys.setlocale('LC_ALL','C')



##################################################################################################################
# Lauch bulk retrieval

system("/home/streamer/dati_cfr/retrieve_data.sh")

##################################################################################################################

##################################################################################################################
# Dataframe management of data

# Define name column
 
names_pluvio=c("IDStaz",
               "Stazione",
			   "Prov",
			   "Zona",
			   "Pcum_1h",
			   "Pcum_p3h",
			   "Pcum_p6h",
			   "Pcum_p12h",
			   "Pcum_p24h",
			   "Pcum_p36h",
			   "HH_Pcum_last")
			   
names_termo=c("IDStaz",
              "Stazione",
			  "Prov",
			  "Area",
			  "Zona",
			  "Elev",
			  "Tair",
			  "HH_t_last",
			  "t_min",
			  "T_HH_t_min",
			  "t_max",
			  "T_HH_t_max",
			  "Y_t_min",
			  "Y_HH_t_min",
			  "Y_t_max",
			  "Y_HH_t_max")
			  
names_igro=c("IDStaz",
             "Stazione",
			 "Provv",
			 "Area",
			 "Zona",
			 "Elev",
			 "RH_last",
			 "HH_RH_last",
			 "HR_min",
			 "T_HH_RH_min",
			 "T_RH_max",
			 "T_HH_RH_max",
			 "Y_RH_min",
			 "Y_HH_RH_min",
			 "Y_HR_max",
			 "Y_HH_RH_max")
			 
names_anemo=c("IDStaz",
              "Stazione",
			  "Prov",
			  "Area",
			  "Zona",
			  "Elev",
			  "v_last",
			  "dir_last",
			  "HH_v_last",
			  "T_v_max",
			  "T_dir_max",
			  "T_HH_max",
			   "Y_v_max",
			  "Y_dir_max",
			  "Y_HH_v_max")

#####################################################################################################################################################‡‡‡

pluvio=read.csv("/home/streamer/dati_cfr/pluvio.csv",header=F);pluvio=pluvio[,1:11]
termo=read.csv("/home/streamer/dati_cfr/termo.csv",header=F);termo=termo[,1:16]
igro=read.csv("/home/streamer/dati_cfr/igro.csv",header=F);igro=igro[,1:16]
anemo=read.csv("/home/streamer/dati_cfr/anemo.csv",header=F);anemo=anemo[,1:16]

names(pluvio)<-names_pluvio
names(termo)<-names_termo
names(igro)<-names_igro
names(anemo)<-names_anemo

##############################################################################################

id=as.numeric(Sys.time())
date_data=as.Date(strptime(paste(pluvio[1,11],"+0100"), "%d/%m %H.%M %z"))
year_data=format(strptime(paste(pluvio[1,11],"+0100"), "%d/%m %H.%M %z"),"%Y")
month_data=format(strptime(paste(pluvio[1,11],"+0100"), "%d/%m %H.%M %z"),"%m")
day_data=format(strptime(paste(pluvio[1,11],"+0100"), "%d/%m %H.%M %z"),"%d")
h_data=format(strptime(paste(pluvio[1,11],"+0100"), "%d/%m %H.%M %z"),"%H")
name_file_rds=paste0("/home/streamer/dati_cfr/data_",year_data,"_",month_data,"_",day_data,"_",h_data,".rds")


pluvio$Date=date_data
termo$Date=date_data
igro$Date=date_data
anemo$Date=date_data

pluvio$id=id
termo$id=id
igro$id=id
anemo$id=id

pluvio$hour=as.numeric(format(strptime(paste(pluvio[,11],"+0100"), "%d/%m %H.%M %z"),"%H"))
termo$hour=floor(as.numeric(termo$HH_t_last))
igro$hour=floor(as.numeric(igro$HH_RH_last))
anemo$hour=floor(as.numeric(anemo$HH_v_last))

pluvio$year=as.numeric(format(strptime(paste(pluvio[,11],"+0100"), "%d/%m %H.%M %z"),"%Y"))
termo$year=year_data
igro$year=year_data
anemo$year=year_data

##################################################################################################
# Join station metadata 

stazioni_lean=readRDS("/home/streamer/dati_cfr/stazioni_regionetoscana_sir.rds")


pluvio_full=sqldf("SELECT IDStaz,X_LAT,Y_LON,Date,id,hour,year,Nome,Comune,Provincia,Quota,Zona,Pcum_1h,Pcum_p3h,Pcum_p6h,Pcum_p12h,Pcum_p24h,Pcum_p36h,HH_Pcum_last FROM pluvio JOIN stazioni_lean ON  pluvio.IDStaz =stazioni_lean.IDStazione")
termo_full=sqldf("SELECT IDStaz,X_LAT,Y_LON,Date,id,hour,year,Nome,Comune,Provincia,Elev,Zona,Tair,HH_t_last,t_min,T_HH_t_min,t_max,T_HH_t_max  FROM termo JOIN stazioni_lean ON  termo.IDStaz =stazioni_lean.IDStazione")
igro_full=sqldf("SELECT IDStaz,X_LAT,Y_LON,Date,id,hour,year,Nome,Comune,Provincia,Elev,Zona,RH_last,HH_RH_last,HR_min,T_HH_RH_min,T_RH_max,T_HH_RH_max FROM igro JOIN stazioni_lean ON  igro.IDStaz =stazioni_lean.IDStazione")
anemo_full=sqldf("SELECT IDStaz,X_LAT,Y_LON,Date,id,hour,year,Nome,Comune,Provincia,Elev,Zona,v_last,dir_last,HH_v_last,T_v_max,T_dir_max,T_HH_max  FROM anemo JOIN stazioni_lean ON  anemo.IDStaz =stazioni_lean.IDStazione")

data_last=list(pluvio_full,termo_full,igro_full,anemo_full)

##################################################################################################
# Save raw data.

saveRDS(data_last,name_file_rds)

write.csv(pluvio_full,"/home/salute/data/output/agro2alert/agro2alert/dati_toscana/pluvio_full_last.csv",row.names=FALSE)
write.csv(termo_full,"/home/salute/data/output/agro2alert/agro2alert/dati_toscana/termo_full_last.csv",row.names=FALSE)
write.csv(igro_full,"/home/salute/data/output/agro2alert/agro2alert/dati_toscana/igro_full_last.csv",row.names=FALSE)
write.csv(anemo_full,"/home/salute/data/output/agro2alert/agro2alert/dati_toscana/anemo_full_last.csv",row.names=FALSE)


##################################################################################################
# Export shapefile

pluvio_last=na.omit(pluvio_full)
coordinates(pluvio_last)= ~Y_LON +X_LAT
proj4string(pluvio_last) <- CRS("+init=epsg:4326") # WGS 84
writeOGR(pluvio_last, ".", "/home/streamer/dati_cfr/pluvio_last",driver="ESRI Shapefile",overwrite_layer=TRUE)

termo_last=na.omit(termo_full)
coordinates(termo_last)= ~Y_LON +X_LAT
proj4string(termo_last) <- CRS("+init=epsg:4326") # WGS 84
writeOGR(termo_last, ".", "/home/streamer/dati_cfr/termo_last",driver="ESRI Shapefile",overwrite_layer=TRUE)


igro_last=na.omit(igro_full)
coordinates(igro_last)= ~Y_LON +X_LAT
proj4string(igro_last) <- CRS("+init=epsg:4326") # WGS 84
writeOGR(igro_last, ".", "/home/streamer/dati_cfr/igro_last",driver="ESRI Shapefile",overwrite_layer=TRUE)


anemo_last=na.omit(anemo_full)
coordinates(anemo_last)= ~Y_LON +X_LAT
proj4string(anemo_last) <- CRS("+init=epsg:4326") # WGS 84
writeOGR(anemo_last, ".", "/home/streamer/dati_cfr/anemo_last",driver="ESRI Shapefile",overwrite_layer=TRUE)

##################################################################################################

if ( file.exists("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/pluvio_last.json")) {file.remove("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/pluvio_last.json")}
if ( file.exists("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/termo_last.json"))  {file.remove("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/termo_last.json")}
if ( file.exists("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/igro_last.json"))   {file.remove("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/igro_last.json")}
if ( file.exists("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/anemo_last.json"))  {file.remove("/home/salute/data/output/agro2alert/agro2alert/dati_toscana/anemo_last.json")}

if ( file.exists("/home/streamer/dati_cfr/pluvio_last.shp"))  {system(" topojson --id-property id_in_shapefile -p -o /home/salute/data/output/agro2alert/agro2alert/dati_toscana/topo_pluvio_last.json -- /home/streamer/dati_cfr/pluvio_last.shp")}
if ( file.exists("/home/streamer/dati_cfr/termo_last.shp"))  {system(" topojson --id-property id_in_shapefile -p -o /home/salute/data/output/agro2alert/agro2alert/dati_toscana/topo_termo_last.json -- /home/streamer/dati_cfr/termo_last.shp")}
if ( file.exists("/home/streamer/dati_cfr/igro_last.shp")) {system(" topojson --id-property id_in_shapefile -p -o /home/salute/data/output/agro2alert/agro2alert/dati_toscana/topo_igro_last.json -- /home/streamer/dati_cfr/igro_last.shp")}
if ( file.exists("/home/streamer/dati_cfr/anemo_last.shp")) {system(" topojson --id-property id_in_shapefile -p -o /home/salute/data/output/agro2alert/agro2alert/dati_toscana/topo_anemo_last.json -- /home/streamer/dati_cfr/anemo_last.shp")}


##################################################################################################


system("ogr2ogr -f \"GeoJSON\" /home/salute/data/output/agro2alert/agro2alert/dati_toscana/pluvio_last.json /home/streamer/dati_cfr/pluvio_last.shp")
system("ogr2ogr -f \"GeoJSON\" /home/salute/data/output/agro2alert/agro2alert/dati_toscana/termo_last.json /home/streamer/dati_cfr/termo_last.shp")
system("ogr2ogr -f \"GeoJSON\" /home/salute/data/output/agro2alert/agro2alert/dati_toscana/igro_last.json /home/streamer/dati_cfr/igro_last.shp")
system("ogr2ogr -f \"GeoJSON\" /home/salute/data/output/agro2alert/agro2alert/dati_toscana/anemo_last.json /home/streamer/dati_cfr/anemo_last.shp")
##################################################################################################

setwd("/home/streamer/dati_cfr/")
file.remove("/home/streamer/dati_cfr/pluvio.csv")
file.remove("/home/streamer/dati_cfr/termo.csv")
file.remove("/home/streamer/dati_cfr/anemo.csv")
file.remove("/home/streamer/dati_cfr/igro.csv")

##################################################################################################


q()

