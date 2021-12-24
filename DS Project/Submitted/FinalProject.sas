/*proc import datafile = "C:\Users\hle4\Downloads\FinalData\DS_Data\Economic.xlsx"
out = WORK.Economic
dbms = xlsx;
run;

proc import datafile = "C:\Users\hle4\Downloads\FinalData\DS_Data\Education.xlsx"
out = WORK.Education
dbms = xlsx;
run;

proc import datafile = "C:\Users\hle4\Downloads\FinalData\DS_Data\Health.xlsx"
out = WORK.Health
dbms = xlsx;
run;

proc import datafile = "C:\Users\hle4\Downloads\FinalData\DS_Data\Population.xlsx"
out = WORK.Population
dbms = xlsx;
run;*/

proc import datafile = "C:\Users\hle4\Downloads\owid_covid_data_final_project_Nov42021.xlsx"
out = WORK.Covid_orig
dbms = xlsx;
run;

proc import datafile = "C:\Users\hle4\Downloads\Excess_Mortality.csv"
out = WORK.Excess_Mortality
dbms = csv replace;
guessingrows = 6900;
run;

data Covid_orig;
retain iso_code continent location date year month;
set Covid_orig;
location = upcase(location);
length year 4 month 3;
year = year(date);
month = month(date);
keep iso_code continent	location date year month total_cases_per_million new_cases_per_million total_vaccinations_per_hundred people_vaccinated_per_hundred people_fully_vacc_per_hundred;
run;

data Excess_Mortality;
set Excess_Mortality;
length year 4 month 3;
run;

proc sort data = Covid_orig; by location year month; run;
proc sort data = Excess_Mortality; by location year month; run;

data Covid_Orig_Updated;
merge Covid_orig Excess_Mortality;
by location year month;
run;

data Covid_Sep_Oct_2021;
set Covid_Orig_Updated;
where (month = 9 or month = 10);
if people_fully_vacc_per_hundred = . or people_fully_vacc_per_hundred <= 0 then delete;
if new_cases_per_million = . or new_cases_per_million <= 0 then delete;
if year ne 2021 then delete;
run;

proc sort data = Covid_Sep_Oct_2021;
by iso_code date;
run;

/*Import dataset together*/
proc import datafile = "C:\Users\hle4\Downloads\Economic.csv"
out = WORK.Economic
dbms = csv replace;
guessingrows = 220;
run;

proc import datafile = "C:\Users\hle4\Downloads\Health.csv"
out = WORK.Health
dbms = csv replace;
guessingrows = 270;
run;

proc import datafile = "C:\Users\hle4\Downloads\Population.csv"
out = WORK.Population
dbms = csv replace;
guessingrows = 440;
run;

proc import datafile = "C:\Users\hle4\Downloads\Unemploy.csv"
out = WORK.Unemploy
dbms = csv replace;
guessingrows = 250;
run;

proc sort data = Economic; by iso_code location; run;
proc sort data = Health; by iso_code location; run;
proc sort data = Population; by iso_code location; run;
proc sort data = Unemploy; by iso_code location; run;

data outsideData;
merge Economic Health Population Unemploy;
by iso_code location;
keep iso_code location gdp_percap economic_year Curr_Health_Exp_GDP Curr_Health_Exp_Pcapita health_year population_year Population__total ages_over_64 unemployment_percent unemploy_year
run;

data Covid_Aggregated;
set Covid_Sep_Oct_2021;
by iso_code;
retain agg_new_cases_per_mil agg_excess_mortality;
if people_fully_vacc_per_hundred = . then people_fully_vacc_per_hundred = 0;
if excess_mortality = . then excess_mortality = 0;
if find(iso_code,"OWID") then delete;
if first.iso_code then do;
	counter = 0;
	agg_new_cases_per_mil = 0;
	agg_excess_mortality = 0;
end;
counter+1;
agg_new_cases_per_mil = agg_new_cases_per_mil + new_cases_per_million;
agg_excess_mortality = agg_excess_mortality + excess_mortality;
if last.iso_code then do;
	avg_test = agg_new_cases_per_mil/counter;
	avg_test2 = agg_excess_mortality/counter;
	output;
end;
run;
