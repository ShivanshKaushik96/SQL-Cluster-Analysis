/*SELECT TOP (1000) [Destination_Latitude]
      ,[Destination_Longitude]
      ,[Total_Demand]
      ,[ClusterNo]
  FROM [Sure Shot (Practice)].[dbo].[2 Cluster Output ]*/


/*For Weighted Lat and Lon*/
select *,Total_Demand*Destination_Latitude as Weighted_Lat,Total_Demand*Destination_Longitude as Weighted_lon
into #Cluster
from [dbo].[2 Cluster Output]

/*For Warehouse new Lat and Lon*/
select *,a.Weighted_Lat/a.Sum_Demand as WH_Lat, a.Weighted_Lon/a.Sum_Demand as WH_Lon
into #New_Lat_Lon
from(
select ClusterNo,COUNT(ClusterNo) as Count_ClusterNo,SUM([Weighted_Lat]) as Weighted_Lat,SUM([Weighted_lon]) as Weighted_Lon, SUM(Total_Demand) as Sum_Demand
from #Cluster
group by ClusterNo
having COUNT(ClusterNo)>15)a

select ROW_NUMBER()over(Order by ClusterNo) as Sno,* from #New_Lat_Lon

/*Joining Source Table By Transposing the New Lat an Lon*/
select * 
into #Data_Cluster
from [dbo].[Source Data]a
cross join (
select Top 1 WH_Lat AS WH_Lat1 ,
WH_Lon AS WH_Lon1,
LEAD(WH_Lat,1)over(order by ClusterNo) as WH_Lat2,
LEAD(WH_Lon,1)over(order by ClusterNo) as WH_Lon2,
LEAD(WH_Lat,2)over(order by ClusterNo) as WH_Lat3,
LEAD(WH_Lon,2)over(order by ClusterNo) as WH_Lon3
from #New_Lat_Lon)b


/*For Calculating Distance Of Warehouse from each store*/
Select *,
2 * 6371* ASIN(SQRT(POWER(((SIN((WH_Lat1*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([WH_Lat1]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([WH_Lon1]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist1,
2 * 6371* ASIN(SQRT(POWER(((SIN((WH_Lat2*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([WH_Lat2]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([WH_Lon2]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist2,
2 * 6371* ASIN(SQRT(POWER(((SIN((WH_Lat3*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([WH_Lat3]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([WH_Lon3]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist3
Into #New
from #Data_Cluster

/*For assigning the Wh No*/
select *,
case 
when a.Min_Dist = a.Dist1 Then 'WH 1'
when a.Min_Dist = a.Dist2 Then 'WH 2'
when a.Min_Dist = a.Dist3 Then 'WH 3'
end as [WH NO]
Into #WH_NO_Table
from(
Select *,
  Case
  When Dist1<Dist2 and Dist1<Dist3 Then Dist1
  When Dist2<Dist1 and Dist2<Dist3 Then Dist2
  When Dist3<Dist2 and Dist3<Dist1 Then Dist3
  End as Min_Dist
from #New)a

/*For Calculating %Deamnd convered by each warehouse*/
select a.*,b.Sum_Total_Demand
into #Output4
from #WH_NO_Table a
inner join (select [WH NO],sum(Cast(replace(Replace(Total_Demand,'$',''),',','')as int))as Sum_Total_Demand
from #WH_NO_Table
group by [WH NO])b
on a.[WH NO]=b.[WH NO]

select *,Cast(replace(Replace(Total_Demand,'$',''),',','')as int)Demand
into #Output5
from #Output4
order by [WH NO]


select *,SUM(a.[Demand%])over(Partition by a.[WH NO] order by a.Min_Dist) as [Cummilative_Demand%]
into #Output6
from(
select *,Cast(Demand as float)/cast(Sum_Total_Demand as float) as [Demand%]
from #Output5
)a

/*For pivotizing the final output*/
Select [WH NO],
COUNT([WH NO]) as No_Of_Stores,
MAX(Min_Dist) as Max_Dist_Covered,
Min(Min_Dist) as Min_Dist_Covered,
Avg(Min_Dist) as Average_Dist_Covered,
cast(SUM(Demand) as float)/cast(145205165 as float)  as [%_Demand_Covered]
from #Output6
Group by [WH NO]

select a.[WH NO],AVG(Dist) as [50%_Demand_Covered_In]
from(
Select *,FIRST_VALUE(Min_Dist)over(Partition by [WH NO] order by Min_Dist) as Dist
from #Output6
where [Cummilative_Demand%]>0.5)a
Group by a.[WH NO]

select a.[WH NO],AVG(Dist) as [80%_Demand_Covered_In]
from(
Select *,FIRST_VALUE(Min_Dist)over(Partition by [WH NO] order by Min_Dist) as Dist
from #Output6
where [Cummilative_Demand%]>0.8)a
Group by a.[WH NO]

select ROW_NUMBER()over(Order by a.[WH NO]) as Sno,a.*,b.[50%_Demand_Covered_In],c.[80%_Demand_Covered_In]
into #Output7
from (Select [WH NO],
COUNT([WH NO]) as No_Of_Stores,
MAX(Min_Dist) as Max_Dist_Covered,
Min(Min_Dist) as Min_Dist_Covered,
Avg(Min_Dist) as Average_Dist_Covered,
cast(SUM(Demand) as float)/cast(145205165 as float)  as [%_Demand_Covered]
from #Output6
Group by [WH NO]
) a
inner join (select a.[WH NO],AVG(Dist) as [50%_Demand_Covered_In]
from(
Select *,FIRST_VALUE(Min_Dist)over(Partition by [WH NO] order by Min_Dist) as Dist
from #Output6
where [Cummilative_Demand%]>0.5)a
Group by a.[WH NO])b
on a.[WH No]=b.[WH NO]
inner join(select a.[WH NO],AVG(Dist) as [80%_Demand_Covered_In]
from(
Select *,FIRST_VALUE(Min_Dist)over(Partition by [WH NO] order by Min_Dist) as Dist
from #Output6
where [Cummilative_Demand%]>0.8)a
Group by a.[WH NO])c
on a.[WH No]=c.[WH NO]

Select a.*,b.WH_Lat,b.WH_Lon
Into #Final_Output_Cluster_Model
from #Output7 a
inner join (select ROW_NUMBER()over(Order by ClusterNo) as Sno,* from #New_Lat_Lon) b
on a.Sno=b.Sno 


/*Final Answer*/
select * from #Final_Output_Cluster_Model
