1. --  how  to remove duplicates 
2. -- standardizing the data  
3. -- Nul values or blank values 
4. -- Remove any columns  

select * from cccc.layoffs_staging 

with cte as ( select * , row_number () over ( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage, country,funds_raised_millions )
 as rn from cccc.layoffs_staging )
select * from cte where rn  > 1 

delete * from cte where rn  > 1 

select * from cccc.layoffs_staging   where company = 'Casper' 

update layoffs_staging2  SET  company=trim(company) ;
select company, trim(company)  from layoffs_staging2  ;
select * from layoffs_staging2 where industry  like 'Crypto%'
update layoffs_staging2  SET  industry = 'Crypto' where industry  like 'Crypto%'




select country, trim(trailing '.' from country)  from layoffs_staging2
update layoffs_staging2  SET  country = trim(trailing '.' from country) where country like 'United States%'; 
select `date` from layoffs_staging2 
update layoffs_staging2  SET `date` = str_to_date(`date`, '%m/%d/%Y')
ALTER TABLE layoffs_staging2  
MODIFY COLUMN `date` DATE ;

select * from layoffs_staging2 where total_laid_off IS NULL and percentage_laid_off IS NULL 
select * from layoffs_staging2 where industry IS NULL
 OR industry = '' ;
 select * from layoffs_staging2 where company = 'Airbnb'
 select * from layoffs_staging2 where company like  'Bally%'
 
 
 
 
 
CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` BIGINT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `rn` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2 
 select * , row_number () over ( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage, country,funds_raised_millions )
 as rn from cccc.layoffs_staging 

select * from layoffs_staging2 where rn > 1
delete from layoffs_staging2 where rn > 1
 
 select distinct (company) from layoffs_staging2  
 
 select * from layoffs_staging2 where industry  like 'Crypto%'
 
 update layoffs_staging2  SET  industry = 'Crypto' where industry  like 'Crypto%'
select distinct country from layoffs_staging2




select distinct country , TRIM(TRAILING '.' FROM country ) from layoffs_staging2 order by 1 
update layoffs_staging2  SET country = TRIM(TRAILING '.' FROM country ) where country LIKE 'United States%'

select t1.industry,t2.industry from layoffs_staging2 as t1 join layoffs_staging2 as t2 
on t1.company=t2.company and t1.location = t2.location
where (t1.industry is null or t1.industry = '') and t2.industry is not null 

update layoffs_staging2 as t1 join layoffs_staging2 as t2 
on t1.company=t2.company
set t1.industry = t2.industry 
where t1.industry is null 
 and t2.industry is not null

update layoffs_staging2 set industry = NULL 
where  industry = ''
select * from layoffs_staging2 where total_laid_off IS NULL and percentage_laid_off IS NULL 
DELETE from layoffs_staging2 where total_laid_off IS NULL and percentage_laid_off IS NULL

ALTER TABLE layoffs_staging2 DROP COLUMN rn 

 select MAX(total_laid_off), MAX(percentage_laid_off) 
 
 select * from layoffs_staging2 where percentage_laid_off = 1 order by total_laid_off DESC
 
select stage,SUM(total_laid_off) as cnt from layoffs_staging2 
group by stage order by 2 DESC 

select MIN(`date`),MAX(`date`) from layoffs_staging2

select company,SUM(percentage_laid_off) as cnt from layoffs_staging2 
group by company order by 2 DESC 

select SUBSTRING(`date`,1,7) as `MONTH ` , SUM(total_laid_off) as total
from layoffs_staging2
where SUBSTRING(`date`,1,7) is not null
group by `MONTH `
order by 1 ASC

WITH Rolling_Total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`,
           SUM(total_laid_off) AS total
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `MONTH`
    ORDER BY `MONTH` ASC
) 
SELECT `MONTH`, total,
       SUM(total) OVER (ORDER BY `MONTH`) AS rolling_total 
FROM Rolling_Total;

WITH cte AS (
    SELECT company, YEAR(`date`) AS year, 
           SUM(total_laid_off) AS total_laid_off,
           DENSE_RANK() OVER (PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC) AS ranking
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
)
SELECT *
FROM cte
WHERE ranking <= 5  AND year IS NOT NULL;

