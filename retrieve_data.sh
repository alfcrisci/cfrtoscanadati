#!/bin/bash

curl  http://www.cfr.toscana.it/monitoraggio/stazioni.php?type=termo | grep "VALUES\[.*new Array" | sed 's/VALUES.*(\"TOS/\"TOS/' | sed 's/);.*$//'  >/home/streamer/dati_cfr/termo.csv
curl  http://www.cfr.toscana.it/monitoraggio/stazioni.php?type=pluvio | grep "VALUES\[.*new Array" | sed 's/VALUES.*(\"TOS/\"TOS/'  | sed 's/);.*$//' | sed  's/.\/b.//g' | sed  's/.b.//g' >/home/streamer/dati_cfr/pluvio.csv
curl  http://www.cfr.toscana.it/monitoraggio/stazioni.php?type=anemo | grep "VALUES\[.*new Array" | sed 's/VALUES.*(\"TOS/\"TOS/'  | sed 's/);.*$//' | sed  's/.\/b.//g' | sed  's/.b.//g' >/home/streamer/dati_cfr/anemo.csv
curl  http://www.cfr.toscana.it/monitoraggio/stazioni.php?type=igro | grep "VALUES\[.*new Array" | sed 's/VALUES.*(\"TOS/\"TOS/'  | sed 's/);.*$//' >/home/streamer/dati_cfr/igro.csv

rm  /home/salute/data/output/agro2alert/agro2alert/dati_toscana/pluvio_last.json 
rm /home/salute/data/output/agro2alert/agro2alert/dati_toscana/termo_last.json 
rm /home/salute/data/output/agro2alert/agro2alert/dati_toscana/igro_last.json 
rm /home/salute/data/output/agro2alert/agro2alert/dati_toscana/anemo_last.json 

