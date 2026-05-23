# Incremental Sales Data Loading Pipeline using Azure Data Factory

## Project Description
This project demonstrates a real-time **Incremental Load Pipeline** using **Watermark Pattern** in Azure Data Factory. It efficiently loads only new or changed records from Azure Data Lake Storage Gen2 into Azure SQL Database instead of doing full load every time.

## Key Features
- Watermark-based Incremental Loading
- Lookup Activity to fetch last loaded date
- Copy Activity with dynamic filter
- Proper error handling and logging
- Reusable pipeline structure

## Architecture
- **Source**: Azure Data Lake Storage Gen2 (CSV files)
- **Sink**: Azure SQL Database
- **Orchestration Tool**: Azure Data Factory (V2)

## Technologies Used
- Azure Data Factory
- Azure Data Lake Storage Gen2 (ADLS)
- Azure SQL Database
- Watermark Pattern

## Screenshots
<img width="236" height="118" alt="image" src="https://github.com/user-attachments/assets/013cb9cf-7d34-4b4a-a4f0-b7cd1aef7bf4" />

<img width="515" height="304" alt="image" src="https://github.com/user-attachments/assets/2a6fd701-9a60-427f-bf19-69eb13ec6148" />

<img width="502" height="331" alt="image" src="https://github.com/user-attachments/assets/6736cfd9-e8c7-402f-b8dd-83e7c0e62cfb" />








<img width="509" height="299" alt="image" src="https://github.com/user-attachments/assets/b0813499-f6af-4ad4-ad1a-145ab3276213" />








## How to Run
1. Update Linked Services with your credentials
2. Update Datasets paths
3. Trigger the pipeline manually or using schedule trigger

## Learnings
- Incremental data loading technique
- Dynamic content and expressions in ADF
- Lookup + Copy Activity pattern for watermark
- Best practices for production-ready pipelines

## Next Projects
- Mapping Data Flow transformations
- Parameterized multi-file processing pipeline
- Microsoft Fabric integration
