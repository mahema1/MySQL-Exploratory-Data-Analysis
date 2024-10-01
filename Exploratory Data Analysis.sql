-- Exploratory Data Analysis 

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers


SELECT * 
FROM layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Which companies had 1 which is basically 100 percent of they company laid off
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2
;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  YEAR(`date`)
ORDER BY 1 DESC;

SELECT substring(`date`, 1,7) as `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT substring(`date`, 1,7) as `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, SUM(total_off) over( ORDER BY `month`) AS rolling_total 
FROM Rolling_total
;
-- Companies with the most Layoffs, Now let's look at that per year.
WITH company_year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), company_year_rank AS
(
SELECT * , DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT * 
FROM company_year_rank
WHERE ranking<=5customer;