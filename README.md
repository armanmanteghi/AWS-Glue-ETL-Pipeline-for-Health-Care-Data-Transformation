# AWS Glue ETL Pipeline for Health Care Data Transformation

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline using AWS Glue to process health care data stored in Amazon S3. The pipeline includes data cleaning, normalization, and transformation, making it ready for downstream analytics or machine learning tasks.

## Features
- Loads data from S3 in CSV format
- Standardizes column names for consistency
- Normalizes text fields to lowercase and trims whitespace
- Handles missing values with appropriate defaults
- Converts date fields to a standard format
- Ensures numerical columns have the correct data types
- Outputs the transformed data back to S3

## Data Processing Steps
1. **Load Data**: The pipeline reads the raw health care data from an S3 bucket.
2. **Standardize Column Names**: Column names are converted to lowercase and spaces are replaced with underscores.
3. **Normalize Text Data**: All text fields are converted to lowercase and trimmed of whitespace.
4. **Handle Missing Values**: Missing values are filled with default values (e.g., "unknown" for categorical fields and 0 for numerical fields).
5. **Convert Date Columns**: The date columns are converted to a standard date format (yyyy-MM-dd).
6. **Ensure Numerical Data Types**: Numerical fields are cast to the appropriate data types (integer for age and room number; double for billing amount).
7. **Correct Inconsistent Data**: Any inconsistencies in categorical data (e.g., gender, admission type) are standardized.
8. **Output Data**: The cleaned and transformed data is saved back to a specified S3 bucket in CSV format.

## Visualizations
This section describes the visualizations created based on the transformed health care data. Each visualization provides insights into the data and supports informed decision-making.

1. **Patient Age Distribution**: 
   - A histogram showing the distribution of patient ages. This visualization helps identify the age groups that are most frequently represented in the data.

2. **Medical Conditions Overview**: 
   - A bar chart illustrating the frequency of various medical conditions. This chart assists in understanding the most common health issues encountered in the patient population.

3. **Billing Amount Analysis**:
   - A box plot displaying the distribution of billing amounts. This visualization highlights outliers and the overall range of billing amounts, providing insights into the financial aspects of health care.

4. **Gender Distribution**: 
   - A pie chart representing the proportion of different genders in the dataset. This chart helps in assessing the demographic diversity of the patient population.

5. **Insurance Providers Analysis**: 
   - A bar chart showing the number of patients associated with different insurance providers. This visualization assists in understanding the market share of various insurance companies.

## Usage
To deploy this ETL pipeline, you need to:
1. Set up an AWS account and configure AWS Glue.
2. Ensure the raw data is available in the specified S3 bucket.
3. Run the provided Glue job script to execute the data transformation.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
