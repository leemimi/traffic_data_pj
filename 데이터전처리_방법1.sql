---처음 전처리 코드---
--테이블만들기 이후 csv 삽입---
create table mydata0107(
기준일자 date,
요일명 varchar(50),
RSE수집시분초 numeric,
RSE_ID numeric,
가상OBU_ID numeric,
DSRC차종구분명 varchar(50),
노선번호 numeric,
도로이정 numeric,
DSRC차종구분코드 numeric);

--데이터 개수 count(안해도됨)--
SELECT COUNT(*) FROM mydata0105;

drop table cnt;
--cnt만들어서 개수 넣어야함!!--
create table cnt as (select 가상OBU_ID, count(*) as cnt from mydata0107 group by 가상OBU_ID);

---into는 테이블 넣는 코드--

--30개 이하 제거--
select 기준일자, 요일명, RSE수집시분초, RSE_ID, a.가상OBU_ID, DSRC차종구분명, 노선번호, 도로이정, DSRC차종구분코드,cnt into s20210107
from mydata0107 as a
left outer join cnt as b
on a.가상OBU_ID = b.가상OBU_ID
where b.cnt >=30 ;

----------여기까지 1차 전처리------------


---전처리1번과정 시작--------

-- 수집시분초 제외 중복제거
select 기준일자,요일명,rse_id,가상obu_id,dsrc차종구분명,노선번호,도로이정,dsrc차종구분코드 into num5_0107
from s20210107 
group by 기준일자,요일명,rse_id,가상obu_id,dsrc차종구분명,노선번호,도로이정,dsrc차종구분코드;



-- rseid 앞뒤 5개 뽑기
select * into obu0107
from num5_0107
where rse_id in(2100501092669, 2100401092629, 2100401092592,2100401092544,2100401092482,
2100501091259,2100501091229, 2100501091194, 2100501091148, 2100501091139 );

drop table cnt1;
--10개인 obu만 추출하기--
create table cnt1 as (select 가상OBU_ID, count(*) as cnt1 from obu0107 group by 가상OBU_ID);



select 기준일자, 요일명, RSE_ID, a.가상OBU_ID, DSRC차종구분명, 노선번호, 도로이정, DSRC차종구분코드,cnt1 into s20210107_7
from mydata0107 as a
right outer join cnt1 as b
on a.가상OBU_ID = b.가상OBU_ID
where b.cnt1 =7 


-- 가상 obuid 목록 뽑기
select 가상obu_id into obu0107_7
from s20210107_7 
group by 가상obu_id;



















-- 만들어졌는지 확인
select *
from obu0101 ;


SELECT COUNT(*) FROM s20210101_7 ;


drop table obu0101_7;


-- 가상 obuid 목록 뽑기
select 가상obu_id into obu0101_7
from s20210101_7 
group by 가상obu_id;

-------여기까지----------


-- 원시테이블에서 obuid 7개 --


select * --into obu0101
from mydata0101
where 가상obu_id in obu0101_7;




select 기준일자, 요일명, RSE_ID, a.가상OBU_ID, DSRC차종구분명, 노선번호, 도로이정, DSRC차종구분코드  --into s20210101_7
from mydata0101 a left join obu0101_7 b  
on a.가상obu_id = b.가상obu_id;

select 기준일자, 요일명, rse수집시분초, RSE_ID, a.가상OBU_ID, DSRC차종구분명, 노선번호, 도로이정, DSRC차종구분코드-- into s20210101_7
from mydata0101  as a
right outer join obu0101_7 as b
on a.가상OBU_ID = b.가상OBU_ID


















select *
from obu0101;

drop table obudata0101  ;








select distinct *
from data20210101;

select distinct 기준일자, 요일명, RSE_ID, 가상OBU_ID, DSRC차종구분명, 노선번호, 도로이정, DSRC차종구분코드 from data20210101 ;   

