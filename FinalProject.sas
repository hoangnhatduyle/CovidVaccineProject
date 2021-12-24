********************************************************************************
|                                                                              *
|University of Toledo Data Science-I										   *
|                                                                              *
|Assignment Number:	Final Project (Due: 12-14)								   *
|                                                                              *
|Input File: Economic.csv													   *
|			 Health.csv														   *
|			 Population.csv													   *
|			 Unemploy.csv													   *
|			 owid_covid_data_final_project_Nov42021							   *
|Date: 12/12/2021															   *
|                                                                              *
********************************************************************************
********************************************************************************
* Programmer: Hoang Nhat Duy Le
* Instructor: David Lilley, PhD
* Purpose: This project is to study the fact that whether or not COVID-19 vaccination rates
* 		   are associated with fewer cases and deaths (excess mortality)?
                                                 
********************************************************************************;

/* Macros */
%macro import(fileName, fileExtension, fileOuput, row);

FILENAME REFFILE "C:\Users\hle4\Downloads\&fileName..&fileExtension";

PROC IMPORT DATAFILE=REFFILE 
DBMS= &fileExtension. OUT=&fileOuput. REPLACE;
guessingrows = &row;
RUN;
%mend;

/* Import original and excess mortality data */
%import(owid_covid_data_final_project_Nov42021, csv, Covid_orig, 20);
%import(Excess_Mortality, csv, excess_mortality, 20);

/* Add 2 new variables: year and month */
data Covid_orig;
retain iso_code continent location date year month;
set Covid_orig;
location = upcase(location);
length year 4 month 3;
year = year(date);
month = month(date);
keep iso_code continent	location date year month new_cases_per_million people_fully_vacc_per_hundred;
run;

/* Change the length of year and month to match the original data before merege */
data Excess_Mortality;
set Excess_Mortality;
length year 4 month 3;
run;

proc sort data = Covid_orig; by location year month; run;
proc sort data = Excess_Mortality; by location year month; run;

/* Merge excess mortality to the original Covid dataset */
data Covid_Orig_Updated;
merge Covid_orig Excess_Mortality;
by location year month;
run;

/* Clean up data, delete any entry with bad data */
data Covid_Sep_Oct_2021;
set Covid_Orig_Updated;
where (month = 9 or month = 10);
if people_fully_vacc_per_hundred = . or people_fully_vacc_per_hundred <= 0 then delete;
if new_cases_per_million = . or new_cases_per_million <= 0 then delete;
if year ne 2021 then delete;
if find(iso_code,"OWID") then delete;	/* not a real country */
run;

proc sort data = Covid_Sep_Oct_2021;
by iso_code date;
run;

/* Aggregate 3 variables: new_case_per_million excess_mortality people_fully_vaccinated */
data Covid_Aggregated;
set Covid_Sep_Oct_2021;
by iso_code;
retain agg_new_cases_per_mil agg_excess_mortality agg_people_fully_vacc;
keep iso_code continent location date year month avg_new_cases_per_mil avg_people_fully_vacc avg_excess_mortality; */
/* if people_fully_vacc_per_hundred = . then people_fully_vacc_per_hundred = 0;
/* if new_cases_per_million = . then new_cases_per_million = 0;
/* if excess_mortality = . then excess_mortality = 0; */
if first.iso_code then do;
	counter = 0;
	agg_new_cases_per_mil = 0;
	agg_people_fully_vacc = 0;
	agg_excess_mortality = 0;
end;
counter+1;
agg_new_cases_per_mil = agg_new_cases_per_mil + new_cases_per_million;
agg_people_fully_vacc = agg_people_fully_vacc + people_fully_vacc_per_hundred;
agg_excess_mortality = agg_excess_mortality + excess_mortality;
if last.iso_code then do;
	avg_new_cases_per_mil = agg_new_cases_per_mil/counter;
	avg_people_fully_vacc = agg_people_fully_vacc/counter;
	avg_excess_mortality = agg_excess_mortality/counter;
	output;
end;
run;

proc sort data = Covid_Aggregated;
by iso_code date;
run;

/*Import supportive dataset */
%import(Economic, csv, Economic, 220);
%import(Health, csv, Health, 270);
%import(Population, csv, Population, 470);
%import(Unemploy, csv, Unemploy, 250);

/* Only keep necessary variables */
data Economic;
set Economic;
keep iso_code location gdp_percap economic_year;
run;

/* Only keep necessary variables */
data Health;
set Health;
keep iso_code location domestic_health_exp_gdp curr_health_exp_gdp health_year;
run;

/* Only keep necessary variables */
data Unemploy;
set Unemploy;
keep iso_code location unemployment_percent unemploy_year;
run;

proc sort data = Economic; by iso_code location; run;
proc sort data = Health; by iso_code location; run;
proc sort data = Unemploy; by iso_code location; run;
proc sort data = Population; by iso_code location; run;

/* Only keep necessary variables. Since this Population dataset is not agggregated yet, I aggregated variables that I chose: total_population and ages_over_64 */
data Population;
set Population;
keep iso_code location population_year avg_total_population avg_ages_over_64;
by iso_code;
retain agg_total_population agg_Ages_over_64;
if first.iso_code then do;
	counter = 0;
	agg_total_population = 0;
	agg_Ages_over_64 = 0;
end;
counter+1;
agg_total_population = agg_total_population + population__total;
agg_Ages_over_64 = agg_Ages_over_64 + Ages_over_64;
if last.iso_code then do;
	avg_total_population = agg_total_population/counter;
	avg_Ages_over_64 = agg_Ages_over_64/counter;
	output;
end;
run;

/* Merge 4 supportive datasets */
data outsideData;
merge Economic (in=a) Health (in=b) Population (in=c) Unemploy (in=d);
by iso_code;
if a = b = c = d;
run;

data outsideData;
retain iso_code location gdp_percap economic_year domestic_health_exp_gdp curr_health_exp_gdp health_year avg_total_population avg_ages_over_64 population_year unemployment_percent unemploy_year;
set outsideData;
run;

/* Merge the covid data with supportive dataset to have mroe control variables */
data finalDataset;
merge Covid_Aggregated (in=a) outsideData (in=b);
by iso_code;
if a=b;
run;

/* Convert domestic_Health curr_health and Unemployment_rate to numeric before running correlation and regression */
data finalDataset;
retain iso_code continent location date year month gdp_percap economic_year domestic_health_exp curr_health_exp health_year avg_total_population avg_ages_over_64 population_year unemployment_rate unemploy_year;
set finalDataset;
domestic_health_exp = domestic_health_exp_gdp*1;
curr_health_exp = curr_health_exp_gdp*1;
unemployment_rate = unemployment_percent*1;
drop domestic_health_exp_gdp curr_health_exp_gdp unemployment_percent;
run;

proc export data = FinalDataset
outfile = "FinalDataset"
dbms = excel;
run;

proc corr data=finalDataset; 
var avg_excess_mortality avg_new_cases_per_mil avg_people_fully_vacc gdp_percap domestic_health_exp curr_health_exp avg_total_population avg_ages_over_64 unemployment_rate;
run;

proc reg data=finalDataset plots=none;
model avg_new_cases_per_mil= avg_people_fully_vacc gdp_percap domestic_health_exp curr_health_exp avg_total_population avg_ages_over_64 unemployment_rate;
run;

proc reg data=finalDataset plots=none;
model avg_excess_mortality= avg_people_fully_vacc gdp_percap domestic_health_exp curr_health_exp avg_total_population avg_ages_over_64 unemployment_rate;
run;
