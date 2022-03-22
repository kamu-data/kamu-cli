// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use chrono::prelude::*;
use kamu::domain;
use opendatafabric as odf;
use opendatafabric::IntoDataStreamBlock;

pub(crate) struct DatasetMetadata {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMetadata {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    fn get_chain(&self, ctx: &Context<'_>) -> Result<Box<dyn domain::MetadataChain>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        Ok(dataset_reg.get_metadata_chain(&self.dataset_handle.as_local_ref())?)
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_handle.clone())
    }

    /// Last recorded watermark
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let chain = self.get_chain(ctx)?;
        Ok(chain
            .iter_blocks_ref(&domain::BlockRef::Head)
            .filter_map(|(_, b)| b.into_data_stream_block())
            .find_map(|b| b.event.output_watermark))
    }

    /// Latest data schema
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<DataSchema> {
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        let schema = query_svc
            .get_schema(&self.dataset_handle.as_local_ref())
            .await?;

        Ok(DataSchema::from_parquet_schema(&schema, format)?)
    }

    /// Current upstream dependencies of a dataset
    async fn current_upstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        let summary = dataset_reg.get_summary(&self.dataset_handle.as_local_ref())?;
        Ok(summary
            .dependencies
            .into_iter()
            .map(|i| {
                Dataset::new(
                    Account::mock(),
                    odf::DatasetHandle::new(i.id.unwrap(), i.name),
                )
            })
            .collect())
    }

    /// Current downstream dependencies of a dataset
    async fn current_downstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();

        // TODO: This is really slow
        Ok(dataset_reg
            .get_all_datasets()
            .filter(|hdl| hdl.id != self.dataset_handle.id)
            .map(|hdl| dataset_reg.get_summary(&hdl.as_local_ref()).unwrap())
            .filter(|sum| {
                sum.dependencies
                    .iter()
                    .any(|i| i.id.as_ref() == Some(&self.dataset_handle.id))
            })
            .map(|sum| Dataset::new(Account::mock(), odf::DatasetHandle::new(sum.id, sum.name)))
            .collect())
    }

    /// Current transformation used by the derivative dataset
    async fn current_transform(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<MetadataEventSetTransform>> {
        use opendatafabric::AsTypedBlock;

        Ok(self
            .get_chain(ctx)?
            .iter_blocks_ref(&domain::BlockRef::Head)
            .filter_map(|(_, b)| b.into_typed::<odf::SetTransform>())
            .next()
            .map(|t| t.event.into()))
    }

    // TODO: MOCK
    async fn current_summary(&self) -> String {
        match self.dataset_handle.name.as_str() {
            "alberta.case-details" | "alberta.case-details.hm" => {
                "Confirmed positive cases of COVID-19 in Alberta."
            }
            "british-columbia.case-details" | "british-columbia.case-details.hm" => {
                "British Columbia COVID-19 case data updated regularly from the B.C. Centre for Disease Control, Provincial Health Services Authority and the B.C. Ministry of Health."
            }
            "ontario.case-details" | "ontario.case-details.hm" => {
                "Confirmed positive cases of COVID-19 in Ontario."
            }
            "quebec.case-details" | "quebec.case-details.hm" => {
                "Confirmed positive cases of COVID-19 in Quebec."
            }
            "canada.case-details" => {
                "Pan-Canadian COVID-19 case data combined from variety of official provincial and municipal data sources."
            }
            "canada.daily-cases" => {
                "Pan-Canadian COVID-19 daily case counts on per Health Region level of aggregation."
            }
            _ => "Default summary",
        }
        .to_string()
    }

    // TODO: MOCK
    async fn current_topics(&self) -> Vec<String> {
        match self.dataset_handle.name.as_str() {
            "alberta.case-details" => {
                vec![
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Pandemic".to_string(),
                    "Disaggregated".to_string(),
                    "Anonymized".to_string(),
                    "Alberta".to_string(),
                    "Canada".to_string(),
                    "Official".to_string(),
                ]
            }
            "alberta.case-details.hm"
            | "british-columbia.case-details.hm"
            | "ontario.case-details.hm"
            | "quebec.case-details.hm" => {
                vec![
                    "COVID-19".to_string(),
                    "Pandemic".to_string(),
                    "Disaggregated".to_string(),
                    "Harmonized".to_string(),
                ]
            }
            "british-columbia.case-details" => {
                vec![
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Pandemic".to_string(),
                    "Disaggregated".to_string(),
                    "Anonymized".to_string(),
                    "British Columbia".to_string(),
                    "Canada".to_string(),
                    "Official".to_string(),
                ]
            }
            "ontario.case-details" => {
                vec![
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Disaggregated".to_string(),
                    "Anonymized".to_string(),
                    "Ontario".to_string(),
                    "Canada".to_string(),
                    "Official".to_string(),
                ]
            }
            "quebec.case-details" => {
                vec![
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Disaggregated".to_string(),
                    "Anonymized".to_string(),
                    "Quebec".to_string(),
                    "Canada".to_string(),
                    "Official".to_string(),
                ]
            }
            "canada.case-details" => {
                vec![
                    "Collection".to_string(),
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Pandemic".to_string(),
                    "Disaggregated".to_string(),
                    "Anonymized".to_string(),
                    "Canada".to_string(),
                ]
            }
            "canada.daily-cases" => {
                vec![
                    "Collection".to_string(),
                    "Healthcare".to_string(),
                    "Epidemiology".to_string(),
                    "COVID-19".to_string(),
                    "Pandemic".to_string(),
                    "Aggregated".to_string(),
                    "Canada".to_string(),
                ]
            }
            _ => vec![
                "Dataset".to_string(),
                "Open Data".to_string(),
                "Research".to_string(),
            ],
        }
    }

    // TODO: MOCK
    async fn current_readme(&self) -> String {
        match self.dataset_handle.name.as_str() {
            "alberta.case-details" => indoc::indoc!(
                r#"
                # Confirmed positive cases of COVID-19 in Alberta

                This dataset compiles daily snapshots of publicly reported data on 2019 Novel Coronavirus (COVID-19) testing in Ontario.

                [Learn how the Government of Ontario is helping to keep Ontarians safe during the 2019 Novel Coronavirus outbreak.](#foo)

                Data includes:
                - approximation of onset date
                - age group
                - patient gender
                - case acquisition information
                - patient outcome
                - reporting Public Health Unit (PHU)
                - postal code, website, longitude, and latitude of PHU

                This dataset is subject to change. Please review the daily epidemiologic summaries for information on variables, methodology, and technical considerations.

                **Related dataset(s)**:
                - [Daily aggregate count of confirmed positive cases of COVID-19 in Alberta](#foo)
                "#
            ),
            "alberta.case-details.hm" => indoc::indoc!(
                r#"
                # Harmonized COVID-19 case data from Alberta

                See [original dataset](#foo).

                See [harmonization schema and semantics](#foo).
                "#
            ),
            "british-columbia.case-details" => indoc::indoc!(
                r#"
                # Confirmed positive cases of COVID-19 in British Columbia

                **Purpose**: These data can be used for visual or reference purposes.

                British Columbia COVID-19 B.C. & Canadian Testing Rates are obtained from the Public Health Agency of Canada's Daily Epidemiologic Update site: https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html.

                These data were made specifically for the British Columbia COVID-19 Dashboard.

                ## Terms of use, disclaimer and limitation of liability

                Although every effort has been made to provide accurate information, the Province of British Columbia, including the British Columbia Centre for Disease Control, the Provincial Health Services Authority and the British Columbia Ministry of Health makes no representation or warranties regarding the accuracy of the information in the dashboard and the associated data, nor will it accept responsibility for errors or omissions. Data may not reflect the current situation, and therefore should only be used for reference purposes. Access to and/or content of these data and associated data may be suspended, discontinued, or altered, in part or in whole, at any time, for any reason, with or without prior notice, at the discretion of the Province of British Columbia.

                Anyone using this information does so at his or her own risk, and by using such information agrees to indemnify the Province of British Columbia, including the British Columbia Centre for Disease Control, the Provincial Health Services Authority and the British Columbia Ministry of Health and its content providers from any and all liability, loss, injury, damages, costs and expenses (including legal fees and expenses) arising from such person's use of the information on this website.

                ## Data Notes - General

                The following data notes define the indicators presented on the public dashboard and describe the data sources involved. Data changes daily as new cases are identified, characteristics of reported cases change or are updated, and data corrections are made. For the latest caveats about the data, please refer to the most recent BCCDC Surveillance Report located at: www.bccdc.ca/health-info/diseases-conditions/covid-19/data

                ## Data Sources

                - Case details and laboratory information are updated daily Monday through Friday at 4:30 pm.
                - Data on cases (including hospitalizations and deaths) is collected by Health Authorities during public health follow-up and provided to BCCDC.
                - Total COVID-19 cases include laboratory diagnosed cases (confirmed and probable) as well as epi-linked cases. Definitions can be found at: www.bccdc.ca/health-professionals/clinical-resources/case-definitions/covid-19-(novel-coronavirus). Prior to June 4, 2020, the total number of cases included only laboratory diagnosed cases. Starting June 4, probable epi-linked cases became reportable as a separate category. Epi-linked cases identified during case investigations since May 19, 2020 - the date BC entered Phase 2 of its Restart Plan - are now included in the case total, but are not considered New Cases unless they were reported in the last 24 hours.
                - Laboratory data is supplied by the B.C. Centre for Disease Control Public Health Laboratory and the Provincial Lab Information Solution (PLIS); tests performed for other provinces have been excluded.
                - Critical care hospitalizations are provided by the health authorities to PHSA on a daily basis. 

                BCCDC/PHSA/B.C. Ministry of Health data sources are available at the links below:

                - [Cases Totals (spatial)](#)
                - [Case Details](#)
                - [Laboratory Testing Information](#)
                - [Regional Summary Data](#)

                ## Data Over Time

                - The number of laboratory tests performed and positivity rate over time are reported by the date of test result. On March 16, testing recommendations changed to focus on hospitalized patients, healthcare workers, long term care facility staff and residents, and those part of a cluster or outbreak who are experiencing respiratory symptoms. The current day is excluded from all laboratory indicators.
                - As of January 7, 2021, the numbers of cases over time are reported by the result date of the client's first positive lab result where available; otherwise by the date they are reported to public health.
                
                ## Epidemiologic Indicators

                - Cases have 'Recovered' when the criteria outlined in BC guidelines for public health management of COVID-19 (www.bccdc.ca/resource-gallery/Documents/Guidelines%20and%20Forms/Guidelines%20and%20Manuals/Epid/CD%20Manual/Chapter%201%20-%20CDC/2019-nCoV-Interim_Guidelines.pdf) are met. These are the same criteria that are met for cases to “Discontinue Isolation” and the terms are sometimes used interchangeably.
                - Today's New Cases are those reported daily in the Provincial Health Officer's press briefing and reflect the difference in counts between one day and the next (net new). This may not be equal to the number of cases identified by calendar day, as: (1) new cases for the current day will be based on lab results up to midnight of the day before; and (2) there may be some delays between cases being reported to public health and then reported provincially; and (3) cases may be attributed to different health authorities or may be excluded from case counts as new information is obtained. 
                - Critical care values include the number of COVID-19 patients in all critical care beds (e.g., intensive care units; high acuity units; and other surge critical care spaces as they become available and/or required). 
                - Active cases exclude those cases who have died, recovered/discontinued isolation or been lost to follow up. 
                - The 7-day moving average is an average daily value over the 7 days up to and including the selected date. The 7-day window moves - or changes - with each new day of data. It is used to smooth new daily case and death counts or rates to mitigate the impact of short-term fluctuations and to more clearly identify the most recent trend over time.
                - The following epidemiological indicators are included in the provincial case data file:
                    - Date: date of the client's first positive lab result where available; otherwise by the date they were reported to public health
                    - HA: health authority assigned to the case
                    - Sex: the sex of the client
                    - Age_Group: the age group of the client
                    - Classification_Reported: whether the case has been lab-diagnosed or is epidemiologically linked to another case
                - The following epidemiological indicators are included in the regional summary data file:
                    - Cases_Reported: the number of cases for the health authority (HA) and health service delivery area (HSDA)
                    - Cases_Reported_Smoothed: Seven day moving average for reported cases

                ## Laboratory Indicators

                - Total tests represent the cumulative number of COVID-19 tests since testing began mid-January. Only tests for residents of B.C. are included.
                - New tests represent the number of COVID-19 tests performed in the 24 hour period prior to date of the dashboard update.
                - COVID-19 positivity rate is calculated for each day as the ratio of 7-day rolling average of number of positive specimens to 7-day rolling average of the total number specimens tested (positive, negative, indeterminate and invalid). A 7-day rolling average applied to all testing data corrects for uneven data release patterns while accurately representing the provincial positivity trends. It avoids misleading daily peaks and valleys due to varying capacities and reporting cadences.
                - Turn-around time is calculated as the daily average time (in hours) between specimen collection and report of a test result. Turn-around time includes the time to ship specimens to the lab; patients who live farther away are expected to have slightly longer average turn around times.
                - The rate of COVID-19 testing per million population is defined as the cumulative number of people tested for COVID-19/BC population x 1,000,000. B.C. and Canadian rates are obtained from the map (Figure 1) available in the Public Health Agency of Canada's Daily Epidemiologic Update: https://health-infobase.canada.ca/covid-19/epidemiological-summary-covid-19-cases.html by selecting Rate and Individuals Tested.  Please note: the same person may be tested multiple times, thus it is not possible to derive this rate directly from the # of cumulative tests reported on the BC COVID dashboard.
                - Testing context:  COVID-19 diagnostic testing and laboratory test guidelines have changed in British Columbia over time.  BC's testing strategy has been characterized by four phases: 1) Exposure-based testing, 2) Targeted testing, 3) Expanded testing, and 4) Symptom-based testing.  While COVID-19 testing was originally centralized at the BCCDC Public Health Laboratory (BCPHL), testing capacity expanded to other BC laboratories over time.  Additional details on the timing and definition of test phases and decentralization of testing can be found at: www.bccdc.ca/health-info/diseases-conditions/covid-19/testing/phases-of-covid-19-testing-in-bc
                - The following laboratory indicators are included in the provincial laboratory data file:
                    - New_Tests: the number of new COVID-19 tests
                    - Total_Tests: the total number of COVID-19 tests
                    - Positivity: the positivity rate for COVID-19 tests
                    - Turn_Around: the turnaround time for COVID-19 tests

                ## Hospitalization Indicators

                - Hospitalizations are defined according to the COVID-19 Case Report Form
                - Hospitalizations are reported by the date of admission. Date of admission is replaced with surveillance date (date of the client's first positive lab result where available; otherwise by the date they were reported to public health) in the rare instance where admission date is missing for a known hospitalization.  
                - Information will change as data becomes available; data from the most recent week, in particular, are incomplete. 

                ## Death Indicators

                - Deaths are defined according to the COVID-19 Case Report Form.
                - Deaths are reported by the date of death. Date of death is replaced with surveillance date (date of the client's first positive lab result where available; otherwise by the date they were reported to public health) in the rare instance where date of death is missing for a known mortality event.
                - Information will change as data becomes available; data from the most recent week, in particular, are incomplete. 

                ## Health Authority Assignment

                - As of July 9, cases are reported by health authority of residence. When health authority of residence is not available, cases are assigned to the health authority reporting the case or the health authority of the provider ordering the lab test. Cases whose primary residence is outside of Canada are reported as “Out of Canada”. Previously, cases were assigned to the health authority that reported the case. Please note that the health authority of residence and the health authority reporting the case do not necessarily indicate the location of exposure or transmission. As more data is collected about the case, the health authority assignment may change to reflect the latest information. 
                - For lab data, health authority is assigned by place of residence; when not available, by location of the provider ordering the lab test. Delays in assignment may occur such that the total number of BC tests performed may be greater than the sum of tests done in specific Health Authorities. 

                © Province of British Columbia 
                "#
            ),
            "british-columbia.case-details.hm" => indoc::indoc!(
                r#"
                # Harmonized COVID-19 case data from British Columbia

                See [original dataset](#foo).

                See [harmonization schema and semantics](#foo).
                "#
            ),
            "ontario.case-details" => indoc::indoc!(
                r#"
                # Confirmed positive cases of COVID-19 in Ontario

                This dataset compiles daily snapshots of publicly reported data on 2019 Novel Coronavirus (COVID-19) testing in Ontario.

                [Learn how the Government of Ontario is helping to keep Ontarians safe during the 2019 Novel Coronavirus outbreak.](#foo)

                Data includes:
                - approximation of onset date
                - age group
                - patient gender
                - case acquisition information
                - patient outcome
                - reporting Public Health Unit (PHU)
                - postal code, website, longitude, and latitude of PHU

                This dataset is subject to change. Please review the daily epidemiologic summaries for information on variables, methodology, and technical considerations.

                **Related dataset(s)**:
                - [Daily aggregate count of confirmed positive cases of COVID-19 in Ontario](#foo)
                "#
            ),
            "ontario.case-details.hm" => indoc::indoc!(
                r#"
                # Harmonized COVID-19 case data from Ontario

                See [original dataset](#foo).

                See [harmonization schema and semantics](#foo).
                "#
            ),
            "quebec.case-details" => indoc::indoc!(
                r#"
                # Confirmed positive cases of COVID-19 in Quebec

                This dataset compiles daily snapshots of publicly reported data on 2019 Novel Coronavirus (COVID-19) testing in Ontario.

                [Learn how the Government of Ontario is helping to keep Ontarians safe during the 2019 Novel Coronavirus outbreak.](#foo)

                Data includes:
                - approximation of onset date
                - age group
                - patient gender
                - case acquisition information
                - patient outcome
                - reporting Public Health Unit (PHU)
                - postal code, website, longitude, and latitude of PHU

                This dataset is subject to change. Please review the daily epidemiologic summaries for information on variables, methodology, and technical considerations.

                **Related dataset(s)**:
                - [Daily aggregate count of confirmed positive cases of COVID-19 in Quebec](#foo)
                "#
            ),
            "quebec.case-details.hm" => indoc::indoc!(
                r#"
                # Harmonized COVID-19 case data from Quebec

                See [original dataset](#foo).

                See [harmonization schema and semantics](#foo).
                "#
            ),
            "canada.daily-cases" => indoc::indoc!(
                r#"
                # Daily aggregate count of confirmed positive cases of COVID-19 in British Columbia
                
                This dataset compiles the aggregate number of daily cases of COVID-19 registered in British Columbia.

                The dataset is based on [ca.bccdc.covid19.case-details](#) dataset, refer to it for the explanation of the data and licensing terms.
                "#
            ),
            "canada.case-details" | "ca.covid19.daily-cases" => indoc::indoc!(
                r#"
                # Epidemiological Data from the COVID-19 Outbreak in Canada

                The [**COVID-19 Canada Open Data Working Group**](https://opencovid.ca/) collects daily time series data on COVID-19 cases, deaths, recoveries, testing and vaccinations at the health region and province levels. Data are collected from publicly available sources such as government datasets and news releases. Updates are made nightly at 22:00 ET. See [`data_notes.txt`](https://github.com/ccodwg/Covid19Canada/blob/master/data_notes.txt) for notes regarding the latest data update. Our data collection is mostly automated; see [`Covid19CanadaETL`](https://github.com/ccodwg/Covid19CanadaETL) for details.

                Our data dashboard is available at the following URL: [https://art-bd.shinyapps.io/covid19canada/](https://art-bd.shinyapps.io/covid19canada/).

                Table of contents:

                * [Accessing the data](#accessing-the-data)
                * [Recent dataset changes](#recent-dataset-changes)
                * [Datasets](#datasets)
                * [Recommended citation](#recommended-citation)
                * [Methodology & data notes](#methodology--data-notes)
                * [Acknowledgements](#acknowledgements)
                * [Contact us](#contact-us)

                ## Accessing the data

                Before using our datasets, please read the [Datasets](#datasets) section below.

                Our datasets are available in three different formats:

                * CSV format from this GitHub repository (to download all the latest data, select the green "Code" button and click "Download ZIP")
                * JSON format from our [API](https://opencovid.ca/api/)
                * [Google Drive](https://drive.google.com/drive/folders/1He6mPAbolgh7jtsq1zu6LpLQKz34n_nP)

                Note that retired datasets (`retired_datasets`) are only available on GitHub.

                ## Recent dataset changes

                * Beginning 2022-02-07, Saskatchewan will only be reporting a limited set of data once per week on Thursdays. See the [news release](https://www.saskatchewan.ca/government/news-and-media/2022/february/03/living-with-covid-transition-of-public-health-management) for more details. This will affect the quality and timeliness of COVID-19 data updates for Saskatchewan.

                ## Datasets

                **Usage notes and caveats**

                The dataset in this repository was launched in March 2020 and has been maintained ever since. As a legacy dataset, it preserves many oddities in the data introduced by changes to COVID-19 reporting over time (see details below). A new, definitive COVID-19 dataset for Canada is currently being developed as [`CovidTimelineCanada`](https://github.com/ccodwg/CovidTimelineCanada), a part of the **[What Happened? COVID-19 in Canada](https://whathappened.coronavirus.icu/)** project. While the new `CovidTimelineCanada` dataset is not yet stable (and thus should not be relied upon), it fixes many of the aforementioned oddities present in the legacy dataset in this repository.

                - See [`data_notes.txt`](https://github.com/ccodwg/Covid19Canada/blob/master/data_notes.txt) for notes regarding issues affecting the dataset.
                - Ontario case, mortality and recovered data are retrieved from individual [public health units](https://www.health.gov.on.ca/en/common/system/services/phu/locations.aspx) (exceptions are listed [here](https://github.com/ccodwg/Covid19Canada/issues/97) and differ from values reported in the [Ontario Ministry of Health dataset](https://data.ontario.ca/dataset/confirmed-positive-cases-of-covid-19-in-ontario/resource/455fd63b-603d-4608-8216-7d8647f43350). For most public health units, we limit cases to confirmed cases (excluding probable cases).
                - Impossible values, such as negative case or death counts
                - Our dataset preserves some "impossible" values such as negative daily case or death counts. This is because our dataset reports primarily the *cumulative* value reported each day by the public health authority. Since historical data are sometimes revised (e.g., cases reassigned to different regions, fixing data quality issues, etc.), this sometimes results in negative values reported for a particular date.
                - Caution regarding testing numbers
                - For continuity, we generally report the first testing number that was reported by the province. For some provinces this was number of tests performed, for others this was number of unique people tested. For the purposes of calculating percent positivity, the number of tests performed should generally be used. The [Public Health Agency of Canada](https://health-infobase.canada.ca/covid-19/epidemiological-summary-covid-19-cases.html) provides a province-level time series of number of tests performed. We supply a compatible version of this dataset as in the [`official_datasets`](https://github.com/ccodwg/Covid19Canada/tree/master/official_datasets) directory as [`phac_n_tests_performed_timeseries_prov.csv`](https://github.com/ccodwg/Covid19Canada/blob/master/official_datasets/can/phac_n_tests_performed_timeseries_prov.csv). This dataset should be used over our dataset for inter-provincial comparisons.
                - Recovered/active case counts are highly problematic
                - The defintion of "recovered" has changed over time and differs between provinces. For example, Quebec changed their defintion of recovered on July 17, 2020, which created a massive spike on that date. For this reason, these data should be interpreted with caution.
                - Recovered and active case numbers for Ontario (and thus Canada) are incorrectly estimated prior to 2021-09-07 and should not be considered reliable.
                - Recovered and active case numbers for British Columbia are no longer available as of 2021-02-10. Values for this province (and thus Canada) should be discarded after this date. Saskatchewan has also stopped reporting these values.

                The update date and time for our dataset is given in [`update_time.txt`](https://github.com/ccodwg/Covid19Canada/blob/master/update_time.txt).

                The following time series data are available at the health region level (as well as at the level of province and Canada-wide):

                * cases (confirmed and probable COVID-19 cases)
                * mortality (confirmed and probable COVID-19 deaths)

                The following time series data are available at the province level (as well as Canada-wide):

                * recovered (COVID-19 cases considered resolved that did not end in death)
                * testing (definitions vary, see our [technical report](https://opencovid.ca/work/technical-report/)
                * active cases (we use the formula *active cases = confirmed cases - recovered - deaths*, which explains the disrepecies between our active case numbers and those reported from official sources)
                * vaccine distribution (total doses distributed)
                * vaccine administration (total doses administered)
                * vaccine completion (second doses administered)
                * vaccine additional doses (third doses administered)

                Note that definitions for each of these values differ between provinces. See our [technical report](https://opencovid.ca/work/technical-report/) for more details.

                Several other important files are also available in the `other` folder:

                * Correspondence between health region names used in our dataset and HRUID values given in Esri Canada's [health region map](https://resources-covid19canada.hub.arcgis.com/datasets/regionalhealthboundaries-1), with [2019 population values](https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1710013401): `other/hr_map.csv`
                * Correspondece between province names used in our dataset and full province names and two-letter abbreviations, with [2019 population values](https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1710013401): `other/prov_map.csv`
                * Correspondece between province names used in our dataset and full province names and two-letter abbreviations, with [2019 population values](https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1710013401) and new Saskatchewan health regions: `other/prov_map_sk_new.csv`
                    * The new Saskatchewan health regions (13 health regions versus 6 in the original data) use *unofficial estimates* of 2020 population values provided by Statistics Canada and may differ from official data released by Statistics Canada at a later date

                We also have a case and mortality datasets which combine our dataset with the official SK provincial dataset using the new 13 reporting zones (our dataset continues to use the old 6 reporting zones) in the `hr_sk_new` folder. Data for SK are only available from August 4, 2020 and onward in this dataset.

                Our individual-level case and mortality datasets are retired as of June 1, 2021 (see `retired_datasets`).

                ## Recommended citation

                Below is the current citation for the dataset:

                * Berry, I., O'Neill, M., Sturrock, S. L., Wright, J. E., Acharya, K., Brankston, G., Harish, V., Kornas, K., Maani, N., Naganathan, T., Obress, L., Rossi, T., Simmons, A. E., Van Camp, M., Xie, X., Tuite, A. R., Greer, A. L., Fisman, D. N., & Soucy, J.-P. R. (2021). A sub-national real-time epidemiological and vaccination database for the COVID-19 pandemic in Canada. Scientific Data, 8(1). doi: https://doi.org/10.1038/s41597-021-00955-2

                Below is the previous citation for the dataset:

                * Berry, I., Soucy, J.-P. R., Tuite, A., & Fisman, D. (2020). Open access epidemiologic data and an interactive dashboard to monitor the COVID-19 outbreak in Canada. Canadian Medical Association Journal, 192(15), E420. doi: https://doi.org/10.1503/cmaj.75262

                ## Methodology & data notes

                Detailed information about our [data collection methodology](https://opencovid.ca/work/dataset/) and [sources](https://opencovid.ca/work/data-sources/), answers to [frequently asked data questions](https://opencovid.ca/work/data-faq/) and the [technical report](https://opencovid.ca/work/technical-report/) for our dataset are available on our [website](https://opencovid.ca/). Note that some of this information is out-of-date and will eventually be updated. Information on automated data collection is available in the [`Covid19CanadaETL`](https://github.com/ccodwg/Covid19CanadaETL) GitHub repository.

                The scripts used to prepare, update and validate the datasets in this repository are available in the [`scripts`](https://github.com/ccodwg/Covid19Canada/tree/master/scripts) folder.

                ## Acknowledgements

                We would like to thank all individuals and organizations across Canada who have worked tirelessly to provide data to the public during this pandemic.

                Additionally, we thank the following organizations/individuals for their support:

                [Public Health Agency of Canada](https://www.canada.ca/en/public-health.html) / Joe Murray ([JMA Consulting](https://jmaconsulting.biz/home))

                ## Contact us

                You can learn more about the COVID-19 Canada Open Data Working Group at [our website](https://opencovid.ca/) and reach out to us via our [contact page](https://opencovid.ca/contact-us/).

                "#
            ),
            _ => "Default readme",
        }
        .to_string()
    }
}
