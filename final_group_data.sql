select t1.latitude
,longitude,quarter,year,sum_etage_hors_sol,sum_nombre_logement,round(avg_annee_construction,2) avg_annee_construction ,sum_superficie_terrain,area,population,dwellings,households,averagehouseholdsize,averageage,averagesizeofcensusfamilies,workers,caserne_count, DATE(cast(year as INT64),cast(((quarter-1)*3+1) as INT64) ,1) as last_date,incendie_count,sum_superficie_batiment,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as incendie_count_last_100,
ifnull((select  sum(INCENDIE_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-300 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as incendie_count_last_300,
ifnull((select  sum(ALARMES_INCENDIES_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as alarm_incendie_count_last_100,
ifnull((select  sum(total_crimes)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as total_crimes_last_100,
ifnull((select  sum(vols_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as vols_count_last_100,
ifnull((select  sum(mefait_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as mefait_count_last_100,
ifnull((select  sum(vol_de_vehicule_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as vol_de_vehicule_count_last_100,
ifnull((select  sum(introduction_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as introduction_count_last_100,
ifnull((select  sum(infractions_entrainant_count)
from  `x-cycling-293802.capstoragedataset2020.CleanData` 
where latitude =t1.latitude
and longitude =t1.longitude
and DATE(cast(year as INT64),cast(month as INT64) ,1) between  DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)-100 and DATE(cast(t1.year as INT64),cast(((quarter-1)*3+1) as INT64) ,1)
),0) as infractions_entrainant_count_last_100
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
min(sum_superficie_batiment) as sum_superficie_batiment,
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




SELECT
  t1.latitude,
  longitude,
  quarter,
  year,
  sum_etage_hors_sol,
  sum_nombre_logement,
       sum_nombre_logement/case when population = 0 then 1 else population end  as  nombre_logement_per_population,
  ROUND(avg_annee_construction,2) avg_annee_construction,
  sum_superficie_terrain,
  area,
  area/case when population = 0 then 1 else population end  as area_per_population,
  population,
  dwellings,
    dwellings/case when population = 0 then 1 else population end  as dwellings_per_population,
  households,
      households/case when population = 0 then 1 else population end  as households_per_population,
  averagehouseholdsize,
  averageage,
  averagesizeofcensusfamilies,
  workers,
        workers/case when population = 0 then 1 else population end  as workers_per_population,
  caserne_count,
 -- DATE(CAST(year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) AS last_date,
  incendie_count,
  sum_superficie_batiment,
          sum_superficie_batiment/case when population = 0 then 1 else population end  as superficie_batiment_per_population,
  ifnull((
    SELECT
      SUM(INCENDIE_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS incendie_count_last_100,
  ifnull((
    SELECT
      SUM(INCENDIE_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-300
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS incendie_count_last_300,
  ifnull((
    SELECT
      SUM(ALARMES_INCENDIES_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS alarm_incendie_count_last_100,
  ifnull((
    SELECT
      SUM(total_crimes)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS total_crimes_last_100,
  ifnull((
    SELECT
      SUM(vols_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS vols_count_last_100,
  ifnull((
    SELECT
      SUM(mefait_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS mefait_count_last_100,
  ifnull((
    SELECT
      SUM(vol_de_vehicule_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS vol_de_vehicule_count_last_100,
  ifnull((
    SELECT
      SUM(introduction_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS introduction_count_last_100,
  ifnull((
    SELECT
      SUM(infractions_entrainant_count)
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData`
    WHERE
      latitude =t1.latitude
      AND longitude =t1.longitude
      AND DATE(CAST(year AS INT64),CAST(month AS INT64),1) BETWEEN DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1)-100
      AND DATE(CAST(t1.year AS INT64),CAST(((quarter-1)*3+1) AS INT64),1) ),
    0) AS infractions_entrainant_count_last_100
FROM (
  SELECT
    latitude,
    longitude,
    quarter,
    year,
    SUM(INCENDIE_count) AS incendie_count,
    SUM(ALARMES_INCENDIES_count) AS alarmes_incendies_count,
    SUM(total_crimes) AS total_crimes,
    SUM(vols_count) AS vols_count,
    SUM(Mefait_count) AS mefait_count,
    SUM(Vol_de_vehicule_count) AS vol_de_vehicule_count,
    SUM(Introduction_count) AS introduction_count,
    SUM(Vol_moteur_count) AS vol_moteur_count,
    SUM(Infractions_entrainant_count) AS infractions_entrainant_count,
    MIN(sum_etage_hors_sol) AS sum_etage_hors_sol,
    MIN(sum_nombre_logement) AS sum_nombre_logement,
    MIN(min_annee_construction) AS min_annee_construction,
    MIN(max_annee_construction) AS max_annee_construction,
    MIN(avg_annee_construction) AS avg_annee_construction,
    MIN(sum_superficie_terrain) AS sum_superficie_terrain,
    MIN(sum_superficie_batiment) AS sum_superficie_batiment,
    MIN(area) AS area,
    MIN(population) AS population,
    MIN(dwellings) AS dwellings,
    MIN(households) AS households,
    MIN(averagehouseholdsize) AS averagehouseholdsize,
    MIN(averageage) AS averageage,
    MIN(averagesizeofcensusfamilies) AS averagesizeofcensusfamilies,
    MIN(workers) AS workers,
    MIN(caserne_count) AS caserne_count
  FROM (
    SELECT
      t.*,
      EXTRACT(QUARTER
      FROM
        DATE(CAST(year AS INT64),CAST(month AS INT64),1)) AS quarter
    FROM
      `x-cycling-293802.capstoragedataset2020.CleanData` t )
  GROUP BY
    latitude,
    longitude,
    quarter,
    year) t1
ORDER BY
  1,
  2,
  4,
  3