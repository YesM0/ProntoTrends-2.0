 ## ProntoTrends - Data Preparation / Data Pipeline Scripts
 This project includes all scripts used for processing and preparing data for ProntoTrends releases.
 
### Data Pipeline Structure

#### [mainProxy.py](Google_Trends/Datapipeline/mainProxy.py)
Contains main scraping capabilities
* features scraping options for 
    * individual keywords (by using /Input Files/Keywords_CC.csv like files) -> only one keyword per request
    * comparisons (by using /Input Files/ProntoPro Trends_Questions_CC.json files) -> comparing different keywords per request (for comparative scaling)
    => Both these filetypes can be generated using Google Sheets or [prepareKeywordsFile.py](Google_Trends/Input_Set-Up/prepareKeywordsFile.py)
* contains proxy settings
* employs multithreading
* usually scrapes for each region of a country
* saves Keyword Level data in:
    * out -> for individual Keywords (Structure: `/out/[KEYWORD]/[DIM]_[COUNTRY]_[REGION-ID]_[KEYWORD]_[KEYWORD-ID].csv`)
    * comparisons -> for comparisons (Structure: `/comparisons/[CATEGORY]/[DIM]_[COUNTRY]_([REGION_ID])_[CATEGORY].csv`)

### [generateSummaries.py](Google_Trends/Datapipeline/generateSummaries.py)
Used to combine multiple Keyword-Level results to a tag / adjust regional data according to relative strength of region
* to prepare scraped data for further processing
* reads in instructions from `/Input-Files/Tag_Keyword_CC.csv` (can can also be created using [prepareKeywordsFile.py](Google_Trends/Input_Set-Up/prepareKeywordsFile.py))
* Use first for merging Keywords to Tags, then rerun for adjustments
* Saves outputs into `/Aggregated/[COUNTRY]/[REGION-ID]_[TAG-ID]_[TAG-NAME]_[DIM].csv`

### [finalCSVgenerator.py](Google_Trends/Datapipeline/finalCSVgenerator.py)
Creates final CSV files for production
* options:
    * createCategoryRegionYearFile: Uses data from `comparisons`, breaks down comparative strength of data by Region and Year (The allowed categories are found in `categories = ['Spend', 'Ceremony Type', 'Reception Location', 'Tags', 'Services', 'Food', 'Wedding Style']` [in Line: 469](Google_Trends/finalCSVgenerator.py:469))
    * createTop5Csv: also uses data from `comparisons`, creates file in format for Top5
    * createMainSectionCsv: accesses the previously created Category-Region-Year files and summarises them in one file 
    * createTagChartData: Uses data from `/Aggregated/[COUNTRY]`, forms it for final trends-chart 
    * createTableData: Reformats data from Tag Chart for the table
    * createMapData: Reformats either Tag Chart Data or uses Geo data from `/Aggregated/[COUNTRY]` for use in the map

### [validateFiles.py](Google_Trends/Validation/validateFiles.py)
Runs validation on final csvs in a chosen Folder. 

Checks:
- columns (names & order)
- labels
- label-counts
- separators
- value-types

Also features functions to fix issues automatically.

RULES TO BE APPLIED ARE SET UP IN [validation_rules.py](Validation/validation_rules.py)

### [inspector.py](Data_Inspectors/inspector.py)
Allows to inspect data in `Aggregated` (Tag-level)

### [comparisons_inspector.py](Data_Inspectors/comparisons_inspector.py)
Allows to inspect data in `comparions` or to assemble `Aggregated` data for a comparison inspection (viewing tags against each other)

## Ideas:
- deeper integration with Production Database
- Various Chart viewer options for visualizing data -> extend comparisons_inspector.py
- presets for flows loadable and selectable -> preselect the choices
- CI to team (non-technical)
