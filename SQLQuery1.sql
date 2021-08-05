SELECT *
FROM Covid..CovidDeaths
ORDER BY 3,4

-- Query death percentage in a country OR odds of one dying of covid
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercent
FROM Covid..CovidDeaths
WHERE location like 'India'
ORDER BY location DESC, date DESC;

-- Infection Rate for a country
SELECT location, date, total_cases, population, (total_cases/population)*100 as infected
FROM Covid..CovidDeaths
WHERE location like 'India'
ORDER BY location , date;

--Query Infetion Rate of all location
SELECT location, date, total_cases, population, (total_cases/population)*100 as infected
FROM Covid..CovidDeaths
ORDER BY infected DESC,location DESC, date DESC;

-- Query Total Deaths of all locations
SELECT location, MAX(total_deaths) as Total_Deaths
FROM Covid..CovidDeaths
WHERE continent is not null
GROUP BY location
ORDER BY MAX(total_deaths) DESC

--Query deaths by Continents
SELECT location, MAX(total_deaths) as Total_Deaths
FROM Covid..CovidDeaths
WHERE continent is null
GROUP BY location
ORDER BY MAX(total_deaths) DESC

--Query names of continent in the dataset
SELECT DISTINCT continent
FROM Covid..CovidDeaths

--Query all Contries with continent
SELECT DISTINCT location, continent
FROM Covid..CovidDeaths
ORDER BY continent

--Query Global Death Percentage
SELECT SUM(new_cases) as TotalCases, SUM(new_deaths) as TotalDeaths, (SUM(new_deaths)/SUM(new_cases))*100 as DeathPercentage
FROM Covid..CovidDeaths
where continent is not null

SELECT location, SUM(CAST(icu_patients as int)) as Total_ICU_Patients
FROM Covid..CovidDeaths
where continent is not null
GROUP BY location
ORDER BY Total_ICU_Patients desc

--Exploring Vaccination Dataset
SELECT * 
FROM Covid..CovidVaccinations
WHERE location like 'India'
ORDER BY date

--Combining Datasets
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, vac.total_vaccinations
INTO Covid..CovidCombined
FROM Covid..CovidDeaths dea
Join Covid..CovidVaccinations vac
	on dea.location = vac.location and
	dea.date = dea.date
where dea.continent is not null
ORDER BY 2,3

-- Query Total vaccinated people in all countries
SELECT location, SUM(CAST(new_vaccinations as bigint)) as TotalVaccinated
FROM Covid..CovidVaccinations
WHERE new_vaccinations is not null
GROUP BY location
ORDER BY location

--Query Rolling vaccinaitons for locations
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Covid..CovidDeaths dea
Join Covid..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3

-- Using CTE to perform Calculation on Partition By in previous query
With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Covid..CovidDeaths dea
Join Covid..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3
)
Select *, (RollingPeopleVaccinated/Population)*100 as PercentVaccinated
From PopvsVac

--Query Vaccinated People percentage for all locations
SELECT location, MAX(CAST(total_vaccinations as bigint)) as TotalVaccinated, population, (MAX(CAST(total_vaccinations as bigint))/population)*100 as percentVaccinated
FROM Covid..CovidCombined
--WHERE location like 'India' 
GROUP BY location, population
ORDER BY percentVaccinated DESC


-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Covid..CovidDeaths dea
Join Covid..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null 
--order by 2,3

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated




-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Covid..CovidDeaths dea
Join Covid..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 