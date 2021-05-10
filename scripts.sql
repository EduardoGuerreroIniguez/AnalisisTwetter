-- Creación de la tabla que almacenara los Tweets recibidos
CREATE TABLE Tweets
(
 id bigint,
 country varchar(100),
 created_at varchar(100),
 favorite_count int,
 followers_count int,
 friends_count int,
 lang varchar(4),
 latitude varchar(20),
 longitude varchar(20),
 retweet_count int,
 screen_name varchar(100),
 text varchar(300),
 text_with_stopwords varchar(300),
 text_without_stopwords varchar(300),
 score_with_stopwords decimal(15,14),
 score_without_stopwords decimal(15,14),
 sentimiento_with_stopword varchar(10),
 sentimiento_without_stopword varchar(10),
 freg datetime not null default getdate()
)


--------------------------------------------------------------------------------------------------------------------------------------------------
--#### DIMENSIONES que se usaran para modelar los tweets y visualizarlos en Power BI####--

--Dim_Terminos contendra los terminos de búsqueda que estamos mandando a buscar en el script python,
--Los indices los creamos en múltiplos de dos dado que al momento de relacionarlos, se los asociara con la suma de sus términos
with Dim_Terminos as(
SELECT 1  as Id, 'MSPowerBI'      AS Term union all 
SELECT 2  as Id, 'qlik'      AS Term union all 
SELECT 4  as Id, 'tableau'      AS Term)
select * into Dim_Terminos from Dim_Terminos

select * from Dim_Terminos;

----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Creamos una tabla que va a tener las combinaciones posibles de términos de búsqueda, esto dado que la vamos a necesitar
-- para consolidar la relación en power bi de los terminos buscados. Recordar que varios términos pueden ser mencionados 
-- en un mismo tweet.
-- Cuando encuentres terminos combinados, su Id será la suma de los Id principales de cada término
with nums as
(
  select 1 as id
  union all 
  select id + 1
    from nums
    where id + 1 < 8
)
,
datos as(
select t.id,
  REPLACE(REPLACE( 
  ltrim(


    iif(cast(t.id & 4 as bit)  = 1, ' tableau','') +
    iif(cast(t.id & 2 as bit)    = 1, ' qlik','') +
    iif(cast(t.id & 1 as bit)  = 1, ' MSPowerBI','')),' ', ' - '), '_',' ')  as Terms
from nums t
)
select * into Dim_AllTerminos from datos option (maxrecursion 0)

select * from Dim_AllTerminos;

with nums as
(
 select 1 as id
 union all 
 select id + 1
 from nums
 where id + 1 < 8
),
datos as(
select t.id
 --,cast(t.id & 64 as bit) as bit7
 --,cast(t.id & 32 as bit) as bit6
 --,cast(t.id & 16 as bit) as bit5
 --,cast(t.id & 8 as bit) as bit4
 ,cast(t.id & 4 as bit) as bit3
 ,cast(t.id & 2 as bit) as bit2
 ,cast(t.id & 1 as bit) as bit1
from nums t
)
 select 
 id, 
 case trend
 when 'bit1' then 1
 when 'bit2' then 2
 when 'bit3' then 4
 --when 'bit4' then 8
 --when 'bit5' then 16
 --when 'bit6' then 32
 --when 'bit7' then 64
 else 0
 end as IdTrend
 into F_MtM_Terms
 From 
 (select * from datos ) p
 UNPIVOT 
 (
 valor for trend in (bit1, bit2, bit3/*, bit4,bit5,bit6,bit7*/)
 ) as unpivo
 where
 valor = 1
option (maxrecursion 0)

-------------------------------------------------------------------------------------------------------------------------

-- Creamos una vista que servirá como tabla de hechos con la información relevante para modelar en Microsoft power BI
-- Nota: al usar vader para análisis de sentimientos, este nos devuelve un score entre -1 y 1, siendo los valores más cercano a menos 1 clasificados como Negativos, los valores cercanos a 0 como neutrales y los valores cercanos a 1 como positivos
create view v_fact_tweets as
SELECT 
  country, favorite_count, followers_count,friends_count
  ,lang,latitude,longitude,retweet_count,screen_name as UserName
  ,text ,text_with_stopwords ,text_without_stopwords
  ,score_with_stopwords,score_without_stopwords
  ,case when score_with_stopwords < 0.4 then 'NEGATIVE'
        when score_with_stopwords < 0.6 then 'NEUTRAL'
		else 'POSITIVE' end as sentimiento_with_stopword
  ,case when score_without_stopwords < 0.4 then 'NEGATIVE'
        when score_without_stopwords < 0.6 then 'NEUTRAL'
		else 'POSITIVE' end as sentimiento_without_stopword
  , freg
  , iif([text] like '%MSPowerBI%' or [text] like '%PowerBI%',1,0) 
  + iif([text] like '%qlik%',2,0) 
  + iif([text] like '%tableau%',4,0)  IdTrend
FROM 
  Tweets
  ;
  
