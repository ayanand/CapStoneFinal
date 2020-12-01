delete  from `x-cycling-293802.capstoragedataset2020.casernes_raw` where date_fin is not null-- limit 10
update  `x-cycling-293802.capstoragedataset2020.casernes_raw` t set longitude_rounded = round(t.longitude,2)  where 1=1
update  `x-cycling-293802.capstoragedataset2020.casernes_raw` t set latitude_rounded = round(t.latitude,2)  where 1=1
select latitude_rounded,longitude_rounded,count(0)
from `x-cycling-293802.capstoragedataset2020.casernes_raw`  
group by latitude_rounded,longitude_rounded

select *   from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw`  limit 10

delete   from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw`  where latitude in (1.0)
This statement removed 31,045 rows from x-cycling-293802:capstoragedataset2020.incidentscrime_raw.
select min(latitude) ,max(latitude), min(longitude), max(longitude)  from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw`  
45.40269124753818
45.70235112097462
-73.96895444690249
-73.48010498471031
select distinct categorie   from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw`   limit 10
1	Vol dans / sur véhicule à moteur
2	Vol de véhicule à moteur
3	Méfait
4	Vols qualifiés
5	Introduction
6	Infractions entrainant la mort


update  `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` t set longitude_rounded = round(t.longitude,2)  where 1=1
update  `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` t set latitude_rounded = round(t.latitude,2)  where 1=1

This statement modified 146,822 rows in x-cycling-293802:capstoragedataset2020.incidentscrime_raw.




select EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )), EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as the_day   from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` t limit 10

select latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )), count(0)   
from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` T
group by latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE ))


select *   from `x-cycling-293802.capstoragedataset2020.fireincidents_raw`  limit 10


select DISTINCT DESCRIPTION_GROUPE  from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'  limit 10

UPDATE `x-cycling-293802.capstoragedataset2020.fireincidents_raw` SET DESCRIPTION_GROUPE = UPPER(DESCRIPTION_GROUPE) WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%' 
This statement modified 83,701 rows in x-cycling-293802:capstoragedataset2020.fireincidents_raw.

select min(latitude) ,max(latitude), min(longitude), max(longitude)   from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'  limit 10
45.402683
45.701391
-73.968954
-73.480086

select  SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1),
SUBSTR(CREATION_DATE_TIME,instr(CREATION_DATE_TIME,'T')+1) from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'  limit 10--  PARSE_DATETIME( "%Y-%m-%dT%I:%M:%S",CREATION_DATE_TIME )


update  `x-cycling-293802.capstoragedataset2020.fireincidents_raw` t set longitude_rounded = round(t.longitude,2)  where 1=1
update  `x-cycling-293802.capstoragedataset2020.fireincidents_raw` t set latitude_rounded = round(t.latitude,2)  where 1=1




select latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR, count(0)   AS COUNT
from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` T
WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'
group by latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) ))







-- fireincidents_v2

with consol as (select latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR, count(0)   AS COUNT
from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` T
WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'
group by latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) ))
)
select t1.* , (select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'INCENDIE') as INCENDIE_count,
(select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'ALARMES-INCENDIES') as ALARMES_INCENDIES_count
from
(select distinct latitude_rounded,longitude_rounded,MONTH,year from consol) t1 




-- crimeIncident
with incidentscrime_raw as (select latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as month,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as year, count(0)    as count
from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` T
group by latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )))
select t1.* , (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vols qualifiés') as vols_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Méfait') as Mefait_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol de véhicule à moteur') as Vol_de_vehicule_count
, (select sum(count) from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Introduction') as Introduction_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol dans / sur véhicule à moteur') as Vol_moteur_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Infractions entrainant la mort') as Infractions_entrainant_count
from 
(select latitude_rounded,longitude_rounded,month,year, sum(count) total_crimes
from incidentscrime_raw
group by  latitude_rounded,longitude_rounded,month,year) t1

-- join crime with fireincidents_raw

with fireincidents as (with consol as (select latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR, count(0)   AS COUNT
from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` T
WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'
group by latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) ))
)
select t1.* , (select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'INCENDIE') as INCENDIE_count,
(select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'ALARMES-INCENDIES') as ALARMES_INCENDIES_count
from
(select distinct latitude_rounded,longitude_rounded,MONTH,year from consol) t1 ),
crimeIncident as (with incidentscrime_raw as (select latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as month,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as year, count(0)    as count
from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` T
group by latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )))
select t1.* , (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vols qualifiés') as vols_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Méfait') as Mefait_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol de véhicule à moteur') as Vol_de_vehicule_count
, (select sum(count) from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Introduction') as Introduction_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol dans / sur véhicule à moteur') as Vol_moteur_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Infractions entrainant la mort') as Infractions_entrainant_count
from 
(select latitude_rounded,longitude_rounded,month,year, sum(count) total_crimes
from incidentscrime_raw
group by  latitude_rounded,longitude_rounded,month,year) t1
)
select  ifnull(fireincidents.latitude_rounded, crimeIncident.latitude_rounded) latitude,
ifnull(fireincidents.longitude_rounded, crimeIncident.longitude_rounded) longitude,
ifnull(fireincidents.month, crimeIncident.month) month,
ifnull(fireincidents.year, crimeIncident.year) year,
INCENDIE_count,
ALARMES_INCENDIES_count,
total_crimes,
vols_count,
Mefait_count,
Vol_de_vehicule_count,
Introduction_count,
Vol_moteur_count,
Infractions_entrainant_count
from fireincidents full outer join crimeIncident on fireincidents.latitude_rounded = crimeIncident.latitude_rounded
and fireincidents.longitude_rounded = crimeIncident.longitude_rounded
and fireincidents.month = crimeIncident.month
and fireincidents.year = crimeIncident.year
 
 -- All joined
 
 with caserne as (select latitude_rounded,longitude_rounded,count(0) caserne_count
from `x-cycling-293802.capstoragedataset2020.casernes_raw`  
group by latitude_rounded,longitude_rounded)
,fireincidents as (with consol as (select latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR, count(0)   AS COUNT
from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` T
WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'
group by latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) ))
)
select t1.* , (select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'INCENDIE') as INCENDIE_count,
(select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'ALARMES-INCENDIES') as ALARMES_INCENDIES_count
from
(select distinct latitude_rounded,longitude_rounded,MONTH,year from consol) t1 ),
crimeIncident as (with incidentscrime_raw as (select latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as month,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as year, count(0)    as count
from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` T
group by latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )))
select t1.* , (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vols qualifiés') as vols_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Méfait') as Mefait_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol de véhicule à moteur') as Vol_de_vehicule_count
, (select sum(count) from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Introduction') as Introduction_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol dans / sur véhicule à moteur') as Vol_moteur_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Infractions entrainant la mort') as Infractions_entrainant_count
from 
(select latitude_rounded,longitude_rounded,month,year, sum(count) total_crimes
from incidentscrime_raw
group by  latitude_rounded,longitude_rounded,month,year) t1
)
select  ifnull(fireincidents.latitude_rounded, crimeIncident.latitude_rounded) latitude,
ifnull(fireincidents.longitude_rounded, crimeIncident.longitude_rounded) longitude,
ifnull(fireincidents.month, crimeIncident.month) month,
ifnull(fireincidents.year, crimeIncident.year) year,
INCENDIE_count,
ALARMES_INCENDIES_count,
total_crimes,
vols_count,
Mefait_count,
Vol_de_vehicule_count,
Introduction_count,
Vol_moteur_count,
Infractions_entrainant_count,
caserne_count
from fireincidents full outer join crimeIncident on (fireincidents.latitude_rounded = crimeIncident.latitude_rounded
and fireincidents.longitude_rounded = crimeIncident.longitude_rounded
and fireincidents.month = crimeIncident.month
and fireincidents.year = crimeIncident.year)
full outer join caserne
on  (ifnull(fireincidents.latitude_rounded, crimeIncident.latitude_rounded) = caserne.latitude_rounded
and ifnull(fireincidents.longitude_rounded, crimeIncident.longitude_rounded) = caserne.longitude_rounded)


select *   from `x-cycling-293802.capstoragedataset2020.property_raw`   limit 10

delete  from `x-cycling-293802.capstoragedataset2020.property_raw` where annee_construction = "9999"
This statement removed 21,954 rows from x-cycling-293802:capstoragedataset2020.property_raw.

select substr(nom_rue,1,instr(nom_rue,'(')-1),t.*   from `x-cycling-293802.capstoragedataset2020.property_raw` t  limit 10


nom_rue_clean	STRING	NULLABLE	
name_arrond	STRING	NULLABLE	
full_address	STRING	NULLABLE	


update `x-cycling-293802.capstoragedataset2020.property_raw` t  set nom_rue_clean = substr(nom_rue,1,instr(nom_rue,'(')-1) where 1=1 




UPDATE `x-cycling-293802.capstoragedataset2020.property_raw`  
SET name_arrond = (select nomofficiel   from `x-cycling-293802.capstoragedataset2020.arrondissements_raw` where coderem =no_arrond_ile_cum )
WHERE 1=1 

This statement modified 467,615 rows in x-cycling-293802:capstoragedataset2020.property_raw.


UPDATE `x-cycling-293802.capstoragedataset2020.property_raw` 
SET full_address = CONCAT(civique_fin," ",nom_rue_clean," ",name_ARROND   ) 
WHERE 1=1

This statement modified 467,615 rows in x-cycling-293802:capstoragedataset2020.property_raw.

update `x-cycling-293802.capstoragedataset2020.property_raw` set full_address = REGEXP_REPLACE(NORMALIZE(full_address, NFD), r'\pM', '') 
where 1=1 

delete  from   `x-cycling-293802.capstoragedataset2020.IncendieProperty` where anneecons = '9999'
--and libelleut like 'Entreposage de tout genre%'
and superfic1 is  null



select distinct latr as latitude,
longr as longitude,
matricule8,
anneecons as ANNEE_CONSTRUCTION,
etagehors as ETAGE_HORS_SOL,
nombrelog as NOMBRE_LOGEMENT ,
superfic1 as SUPERFICIE_BATIMENt,
superficie as SUPERFICIE_TERRAIN
from `x-cycling-293802.capstoragedataset2020.IncendieProperty`
where LatR= '45.44'
and longR='-73.97' --limit 10


 create table `x-cycling-293802.capstoragedataset2020.PropertyClean` as
select round(lat,2) latitude,
 round(long,2) longitude,
 sum(ifnull(cast(etage_hors_sol as INT64),0)) sum_etage_hors_sol,
 sum(ifnull(cast(nombre_logement as INT64),0)) sum_nombre_logement,
 min(annee_construction) min_annee_construction,
 max(annee_construction) max_annee_construction,
 avg(cast(annee_construction as INT64)) avg_annee_construction,
 sum(cast(superficie_terrain as INT64)) sum_superficie_terrain,
 sum(cast(superficie_batiment as INT64)) sum_superficie_batiment
from `x-cycling-293802.capstoragedataset2020.property_geo` a,`x-cycling-293802.capstoragedataset2020.property_raw` b
where a.uid = b.id_uev
group by round(lat,2) , round(long,2)
 limit 10
 
 
 
 create table `x-cycling-293802.capstoragedataset2020.fireDataClean` as
with caserne as (select latitude_rounded,longitude_rounded,count(0) caserne_count
from `x-cycling-293802.capstoragedataset2020.casernes_raw`  
group by latitude_rounded,longitude_rounded)
,fireincidents as (with consol as (select latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR, count(0)   AS COUNT
from `x-cycling-293802.capstoragedataset2020.fireincidents_raw` T
WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%'
group by latitude_rounded,longitude_rounded,description_groupe,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) ))
)
select t1.* , (select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'INCENDIE') as INCENDIE_count,
(select count from consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.DESCRIPTION_GROUPE = 'ALARMES-INCENDIES') as ALARMES_INCENDIES_count
from
(select distinct latitude_rounded,longitude_rounded,MONTH,year from consol) t1 ),
crimeIncident as (with incidentscrime_raw as (select latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as month,EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )) as year, count(0)    as count
from `x-cycling-293802.capstoragedataset2020.incidentscrime_raw` T
group by latitude_rounded,longitude_rounded,categorie,EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",t.DATE )),EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",t.DATE )))
select t1.* , (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vols qualifiés') as vols_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Méfait') as Mefait_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol de véhicule à moteur') as Vol_de_vehicule_count
, (select sum(count) from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Introduction') as Introduction_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Vol dans / sur véhicule à moteur') as Vol_moteur_count
, (select count from incidentscrime_raw consol where t1.latitude_rounded=consol.latitude_rounded and  t1.longitude_rounded=consol.longitude_rounded and  t1.MONTH=consol.MONTH and  t1.year=consol.year
and consol.categorie = 'Infractions entrainant la mort') as Infractions_entrainant_count
from 
(select latitude_rounded,longitude_rounded,month,year, sum(count) total_crimes
from incidentscrime_raw
group by  latitude_rounded,longitude_rounded,month,year) t1
)
select  ifnull(fireincidents.latitude_rounded, crimeIncident.latitude_rounded) latitude,
ifnull(fireincidents.longitude_rounded, crimeIncident.longitude_rounded) longitude,
ifnull(fireincidents.month, crimeIncident.month) month,
ifnull(fireincidents.year, crimeIncident.year) year,
INCENDIE_count,
ALARMES_INCENDIES_count,
total_crimes,
vols_count,
Mefait_count,
Vol_de_vehicule_count,
Introduction_count,
Vol_moteur_count,
Infractions_entrainant_count,
caserne_count
from fireincidents full outer join crimeIncident on (fireincidents.latitude_rounded = crimeIncident.latitude_rounded
and fireincidents.longitude_rounded = crimeIncident.longitude_rounded
and fireincidents.month = crimeIncident.month
and fireincidents.year = crimeIncident.year)
full outer join caserne
on  (ifnull(fireincidents.latitude_rounded, crimeIncident.latitude_rounded) = caserne.latitude_rounded
and ifnull(fireincidents.longitude_rounded, crimeIncident.longitude_rounded) = caserne.longitude_rounded)




select * from `x-cycling-293802.capstoragedataset2020.censusId`

select * from `x-cycling-293802.capstoragedataset2020.CensusRaw` where workers = 'x'

update  `x-cycling-293802.capstoragedataset2020.CensusRaw` set workers = '0'  where workers = 'x' 
update  `x-cycling-293802.capstoragedataset2020.CensusRaw` set area = '0'  where area = 'x' 


select geouid,
area,
population,
dwellings,
households,
averagehouseholdsize,
averageage,
averagesizeofcensusfamilies,
ifnull(cast(workers as INT64),0) as workers 
from `x-cycling-293802.capstoragedataset2020.CensusRaw` 


delete
from `x-cycling-293802.capstoragedataset2020.CensusRaw` 
where workers = '..'

select geouid,
ifnull(cast(area as FLOAT64),0) as area ,
ifnull(cast(population as INT64),0) as population ,
ifnull(cast(dwellings as INT64),0) as dwellings ,
ifnull(cast(households as INT64),0) as households ,
ifnull(cast(averagehouseholdsize as FLOAT64),0) as averagehouseholdsize ,
ifnull(cast(averageage as FLOAT64),0) as averageage ,
ifnull(cast(averagesizeofcensusfamilies as FLOAT64),0) as averagesizeofcensusfamilies ,
ifnull(cast(workers as INT64),0) as workers,
lat,
long
from `x-cycling-293802.capstoragedataset2020.CensusRaw` ,`x-cycling-293802.capstoragedataset2020.censusId` 
where id = geouid


create table `x-cycling-293802.capstoragedataset2020.CensusClean` as
with t as (select geouid,
ifnull(cast(area as FLOAT64),0) as area ,
ifnull(cast(population as INT64),0) as population ,
ifnull(cast(dwellings as INT64),0) as dwellings ,
ifnull(cast(households as INT64),0) as households ,
ifnull(cast(averagehouseholdsize as FLOAT64),0) as averagehouseholdsize ,
ifnull(cast(averageage as FLOAT64),0) as averageage ,
ifnull(cast(averagesizeofcensusfamilies as FLOAT64),0) as averagesizeofcensusfamilies ,
ifnull(cast(workers as INT64),0) as workers,
lat,
long
from `x-cycling-293802.capstoragedataset2020.CensusRaw` ,`x-cycling-293802.capstoragedataset2020.censusId` 
where id = geouid)
select lat, long, sum(area) area, sum(population) population, sum(dwellings) dwellings, sum(households) households, sum(averagehouseholdsize) averagehouseholdsize, sum(averageage) averageage, sum(averagesizeofcensusfamilies) averagesizeofcensusfamilies,sum(workers) workers
from t 
group by lat , long



select * from `x-cycling-293802.capstoragedataset2020.CensusClean`

select * from `x-cycling-293802.capstoragedataset2020.fireDataClean`-- latitude , longitude

select * from `x-cycling-293802.capstoragedataset2020.PropertyClean` -- latitude , longitude



select * 
from `x-cycling-293802.capstoragedataset2020.fireDataClean` x
left outer join `x-cycling-293802.capstoragedataset2020.PropertyClean` y
on x.latitude = y.longitude
and  x.longitude = y.latitude


final data 



select x.latitude	,
x.longitude	,
month	,
year	,
ifnull(INCENDIE_count,0)	INCENDIE_count,
ifnull(ALARMES_INCENDIES_count,0)	ALARMES_INCENDIES_count,
ifnull(total_crimes,0)	total_crimes,
ifnull(vols_count,0)	vols_count,
ifnull(Mefait_count,0)	Mefait_count,
ifnull(Vol_de_vehicule_count,0)	Vol_de_vehicule_count,
ifnull(Introduction_count,0)	Introduction_count,
ifnull(Vol_moteur_count,0)	Vol_moteur_count,
ifnull(Infractions_entrainant_count,0)	Infractions_entrainant_count,
ifnull(caserne_count,0)	caserne_count,
sum_etage_hors_sol	,
sum_nombre_logement	,
min_annee_construction	,
max_annee_construction	,
avg_annee_construction	,
sum_superficie_terrain	,
sum_superficie_batiment	,
area	,
population	,
dwellings	,
households	,
averagehouseholdsize	,
averageage	,
averagesizeofcensusfamilies	,
workers	
from `x-cycling-293802.capstoragedataset2020.fireDataClean` x
left outer join `x-cycling-293802.capstoragedataset2020.PropertyClean` y
on x.latitude = y.longitude
and  x.longitude = y.latitude
left outer join  `x-cycling-293802.capstoragedataset2020.CensusClean` z
on x.latitude = z.lat
and  x.longitude = z.long
where y.sum_superficie_batiment is not null
and population is not null
order by 1,2,3,4




--------------**
create table `x-cycling-293802.capstoragedataset2020.space` as
select distinct latitude_rounded,longitude_rounded    from `x-cycling-293802.capstoragedataset2020.fireincidents_raw`


create table `x-cycling-293802.capstoragedataset2020.period` as
SELECT T.*, DATE(cast(year as INT64),cast(month as INT64) ,1)-1 as date_ymd
FROM 
(SELECT DISTINCT EXTRACT(MONTH FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1) )) AS MONTH ,
EXTRACT(YEAR FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS YEAR,
EXTRACT(QUARTER FROM PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))) AS QUARTER 
--,LAST_DAY(DATE_SUB(( PARSE_DATE( "%Y-%m-%d",SUBSTR(CREATION_DATE_TIME,1,instr(CREATION_DATE_TIME,'T')-1))),INTERVAL 31 DAY)) LAST_PERIOD
FROM
      `x-cycling-293802.capstoragedataset2020.fireincidents_raw`
      WHERE UPPER(DESCRIPTION_GROUPE) LIKE '%INCENDIE%') T 
      ORDER BY 2,1
	  
	  
------------------ gives all latitides and longitudes + periods	  
select s.*,t.* 
from `x-cycling-293802.capstoragedataset2020.period` s, `x-cycling-293802.capstoragedataset2020.space` t
order by date_ymd ,latitude_rounded,longitude_rounded


create table `x-cycling-293802.capstoragedataset2020.CleanData` as 
select x.latitude	,
x.longitude	,
month	,
year	,
ifnull(INCENDIE_count,0)	INCENDIE_count,
ifnull(ALARMES_INCENDIES_count,0)	ALARMES_INCENDIES_count,
ifnull(total_crimes,0)	total_crimes,
ifnull(vols_count,0)	vols_count,
ifnull(Mefait_count,0)	Mefait_count,
ifnull(Vol_de_vehicule_count,0)	Vol_de_vehicule_count,
ifnull(Introduction_count,0)	Introduction_count,
ifnull(Vol_moteur_count,0)	Vol_moteur_count,
ifnull(Infractions_entrainant_count,0)	Infractions_entrainant_count,
ifnull(caserne_count,0)	caserne_count,
sum_etage_hors_sol	,
sum_nombre_logement	,
min_annee_construction	,
max_annee_construction	,
avg_annee_construction	,
sum_superficie_terrain	,
sum_superficie_batiment	,
area	,
population	,
dwellings	,
households	,
averagehouseholdsize	,
averageage	,
averagesizeofcensusfamilies	,
workers	
from `x-cycling-293802.capstoragedataset2020.fireDataClean` x
left outer join `x-cycling-293802.capstoragedataset2020.PropertyClean` y
on x.latitude = y.longitude
and  x.longitude = y.latitude
left outer join  `x-cycling-293802.capstoragedataset2020.CensusClean` z
on x.latitude = z.lat
and  x.longitude = z.long
where y.sum_superficie_batiment is not null
and population is not null
order by 1,2,3,4



select latitude ,	longitude ,	quarter ,	year ,	sum(INCENDIE_count) as incendie_count, 	sum(ALARMES_INCENDIES_count) as alarmes_incendies_count,
sum(total_crimes) as total_crimes,
sum(vols_count) as vols_count ,
sum(Mefait_count) as mefait_count,
sum(Vol_de_vehicule_count) as vol_de_vehicule_count,
sum(Introduction_count) as introduction_count,
sum(Vol_moteur_count) as vol_moteur_count,
sum(Infractions_entrainant_count) as infractions_entrainant_count,
min(sum_etage_hors_sol) as sum_etage_hors_sol,
min(sum_nombre_logement) as sum_nombre_logement,
min(min_annee_construction) as min_annee_construction,
min(max_annee_construction) as max_annee_construction,
min(avg_annee_construction) as avg_annee_construction,
min(sum_superficie_terrain) as sum_superficie_terrain,
min(area) as area,
min(population) as population,
min(dwellings) as dwellings,
min(households) as households,
min(averagehouseholdsize) as averagehouseholdsize,
min(averageage) as averageage,
min(averagesizeofcensusfamilies) as averagesizeofcensusfamilies,
min(workers) as workers,
min(caserne_count) as caserne_count
from
(
select t.*,EXTRACT(QUARTER FROM DATE(cast(year as INT64),cast(month as INT64) ,1)) as quarter from `x-cycling-293802.capstoragedataset2020.CleanData` t
)
group by latitude ,	longitude ,	quarter ,	year
order by 1,2,4,3
	  
	  
45.52
-73.57
6
2020
7  


select t.*,  DATE(cast(year as INT64),cast(month as INT64) ,1) from  `x-cycling-293802.capstoragedataset2020.CleanData` t
where latitude =45.52
and longitude =-73.57
and year =2019



select * from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =45.52
and longitude =-73.57
and year =2020



select t1.*, DATE(cast(year as INT64),cast(quarter*3 as INT64) ,1) as last_date
from 
(select latitude ,	longitude ,	quarter ,	year ,	sum(INCENDIE_count) as incendie_count, 	sum(ALARMES_INCENDIES_count) as alarmes_incendies_count,
sum(total_crimes) as total_crimes,
sum(vols_count) as vols_count ,
sum(Mefait_count) as mefait_count,
sum(Vol_de_vehicule_count) as vol_de_vehicule_count,
sum(Introduction_count) as introduction_count,
sum(Vol_moteur_count) as vol_moteur_count,
sum(Infractions_entrainant_count) as infractions_entrainant_count,
min(sum_etage_hors_sol) as sum_etage_hors_sol,
min(sum_nombre_logement) as sum_nombre_logement,
min(min_annee_construction) as min_annee_construction,
min(max_annee_construction) as max_annee_construction,
min(avg_annee_construction) as avg_annee_construction,
min(sum_superficie_terrain) as sum_superficie_terrain,
min(area) as area,
min(population) as population,
min(dwellings) as dwellings,
min(households) as households,
min(averagehouseholdsize) as averagehouseholdsize,
min(averageage) as averageage,
min(averagesizeofcensusfamilies) as averagesizeofcensusfamilies,
min(workers) as workers,
min(caserne_count) as caserne_count
from
(
select t.*,EXTRACT(QUARTER FROM DATE(cast(year as INT64),cast(month as INT64) ,1)) as quarter from `x-cycling-293802.capstoragedataset2020.CleanData` t
)
group by latitude ,	longitude ,	quarter ,	year) t1
order by 1,2,4,3


select  sum(INCENDIE_count), sum(ALARMES_INCENDIES_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =45.52
and longitude =-73.57
and DATE(cast(year as INT64),cast(month as INT64) ,1) between date(2020,1,1)-100 and date(2020,1,1)



select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =45.52
and longitude =-73.57
and DATE(cast(year as INT64),cast(month as INT64) ,1) between date(2020,1,1)-100 and date(2020,1,1)


select t1.*, DATE(cast(year as INT64),cast(quarter*3 as INT64) ,1) as last_date,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)
),0) as incendie_count_last_100,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)-300 and DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)
),0) as incendie_count_last_300,
ifnull((select  sum(ALARMES_INCENDIES_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(t1.quarter*3 as INT64) ,1)
),0) as alarm_incendie_count_last_100
from 
(select latitude ,	longitude ,	quarter ,	year ,	sum(INCENDIE_count) as incendie_count, 	sum(ALARMES_INCENDIES_count) as alarmes_incendies_count,
sum(total_crimes) as total_crimes,
sum(vols_count) as vols_count ,
sum(Mefait_count) as mefait_count,
sum(Vol_de_vehicule_count) as vol_de_vehicule_count,
sum(Introduction_count) as introduction_count,
sum(Vol_moteur_count) as vol_moteur_count,
sum(Infractions_entrainant_count) as infractions_entrainant_count,
min(sum_etage_hors_sol) as sum_etage_hors_sol,
min(sum_nombre_logement) as sum_nombre_logement,
min(min_annee_construction) as min_annee_construction,
min(max_annee_construction) as max_annee_construction,
min(avg_annee_construction) as avg_annee_construction,
min(sum_superficie_terrain) as sum_superficie_terrain,
min(area) as area,
min(population) as population,
min(dwellings) as dwellings,
min(households) as households,
min(averagehouseholdsize) as averagehouseholdsize,
min(averageage) as averageage,
min(averagesizeofcensusfamilies) as averagesizeofcensusfamilies,
min(workers) as workers,
min(caserne_count) as caserne_count
from
(
select t.*,EXTRACT(QUARTER FROM DATE(cast(year as INT64),cast(month as INT64) ,1)) as quarter from `x-cycling-293802.capstoragedataset2020.CleanData` t
)
group by latitude ,	longitude ,	quarter ,	year) t1
order by 1,2,4,3


-- quarter correct


select t1.*, DATE(cast(year as INT64),cast(((quarter-1)*3+1) as INT64) ,1) as last_date,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)
),0) as incendie_count_last_100,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)-300 and DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)
),0) as incendie_count_last_300,
ifnull((select  sum(ALARMES_INCENDIES_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((t1.quarter-1)*3+1) as INT64) ,1)
),0) as alarm_incendie_count_last_100
from 
(select latitude ,	longitude ,	quarter ,	year ,	sum(INCENDIE_count) as incendie_count, 	sum(ALARMES_INCENDIES_count) as alarmes_incendies_count,
sum(total_crimes) as total_crimes,
sum(vols_count) as vols_count ,
sum(Mefait_count) as mefait_count,
sum(Vol_de_vehicule_count) as vol_de_vehicule_count,
sum(Introduction_count) as introduction_count,
sum(Vol_moteur_count) as vol_moteur_count,
sum(Infractions_entrainant_count) as infractions_entrainant_count,
min(sum_etage_hors_sol) as sum_etage_hors_sol,
min(sum_nombre_logement) as sum_nombre_logement,
min(min_annee_construction) as min_annee_construction,
min(max_annee_construction) as max_annee_construction,
min(avg_annee_construction) as avg_annee_construction,
min(sum_superficie_terrain) as sum_superficie_terrain,
min(area) as area,
min(population) as population,
min(dwellings) as dwellings,
min(households) as households,
min(averagehouseholdsize) as averagehouseholdsize,
min(averageage) as averageage,
min(averagesizeofcensusfamilies) as averagesizeofcensusfamilies,
min(workers) as workers,
min(caserne_count) as caserne_count
from
(
select t.*,EXTRACT(QUARTER FROM DATE(cast(year as INT64),cast(month as INT64) ,1)) as quarter from `x-cycling-293802.capstoragedataset2020.CleanData` t
)
group by latitude ,	longitude ,	quarter ,	year) t1
order by 1,2,4,3


