# Modern Data Warehouse with SQL Server

A comprehensive implementation of a modern data warehouse architecture using SQL Server, featuring a medallion (Bronze-Silver-Gold) layered approach for scalable data processing and analytics.

## 🏗️ Architecture Overview

This project implements a three-tier medallion architecture:

- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned and standardized data with quality validations
- **Gold Layer**: Analytics-ready dimensional models (star schema)

## 📁 Project Structure

```
├── scripts/
│   ├── init_database.sql           # Database and schema initialization
│   ├── bronze/
│   │   ├── ddl_bronze.sql          # Bronze layer table definitions
│   │   └── proc_load_bronze.sql    # Data loading procedures for Bronze
│   ├── silver/
│   │   ├── ddl_silver.sql          # Silver layer table definitions
│   │   └── proc_load_silver.sql    # Data transformation and loading for Silver
│   └── gold/
│       └── ddl_gold.sql            # Gold layer views (star schema)
├── tests/
│   ├── quality_checks_silver.sql   # Data quality validations for Silver layer
│   └── quality_checks_gold.sql     # Data integrity checks for Gold layer
└── documents/
    ├── architecture_diagram.png    # System architecture overview
    ├── data_flow_diagram.png       # Data processing flow
    ├── data_model.png              # Dimensional model diagram
    └── data_catalog.md             # Data dictionary and catalog
```

## 🚀 Getting Started

### Prerequisites

- SQL Server 2019 or later
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Appropriate permissions to create databases and schemas

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Daniel-jcVv/sql-data-warehouse.git
   cd sql-data-warehouse
   ```

2. **Initialize the database**
   ```sql
   -- Execute the database initialization script
   sqlcmd -S your_server -i scripts/init_database.sql
   ```

3. **Deploy Bronze Layer**
   ```sql
   -- Create Bronze tables
   sqlcmd -S your_server -d your_database -i scripts/bronze/ddl_bronze.sql
   
   -- Deploy Bronze loading procedures
   sqlcmd -S your_server -d your_database -i scripts/bronze/proc_load_bronze.sql
   ```

4. **Deploy Silver Layer**
   ```sql
   -- Create Silver tables with metadata columns
   sqlcmd -S your_server -d your_database -i scripts/silver/ddl_silver.sql
   
   -- Deploy Silver transformation procedures
   sqlcmd -S your_server -d your_database -i scripts/silver/proc_load_silver.sql
   ```

5. **Deploy Gold Layer**
   ```sql
   -- Create Gold layer views (dimensional model)
   sqlcmd -S your_server -d your_database -i scripts/gold/ddl_gold.sql
   ```

## 🔄 Data Processing Flow

### Bronze Layer
- **Purpose**: Raw data ingestion preserving original structure
- **Features**: 
  - Replicates source system structures
  - Truncate and insert loading pattern
  - Error handling and duration monitoring
  - Minimal data validation

### Silver Layer
- **Purpose**: Cleaned and standardized business data
- **Features**:
  - Data cleansing and standardization
  - Metadata columns (`dw_create_date`, etc.)
  - Enhanced error handling
  - Data quality validations

### Gold Layer
- **Purpose**: Analytics-ready dimensional model
- **Features**:
  - Star schema design (facts and dimensions)
  - Optimized for reporting and analytics
  - Business-friendly views
  - Aggregated and calculated metrics

## ✅ Data Quality Management

### Silver Layer Quality Checks
```sql
-- Execute Silver layer quality validations
sqlcmd -S your_server -d your_database -i tests/quality_checks_silver.sql
```

### Gold Layer Quality Checks
```sql
-- Execute Gold layer integrity validations
sqlcmd -S your_server -d your_database -i tests/quality_checks_gold.sql
```

## 📊 Key Features

- **Medallion Architecture**: Bronze-Silver-Gold layered approach
- **Error Handling**: Comprehensive error management and logging
- **Performance Monitoring**: Built-in duration tracking for ETL processes
- **Data Quality**: Automated quality checks and validations
- **Metadata Management**: Tracking columns for data lineage
- **Dimensional Modeling**: Star schema optimized for analytics
- **Scalable Design**: Modular structure for easy maintenance

## 🛠️ Stored Procedures

### Bronze Layer Procedures
- **`proc_load_bronze`**: Handles raw data ingestion with error handling and performance tracking

### Silver Layer Procedures  
- **`proc_load_silver`**: Performs data transformation, cleansing, and standardization

## 📈 Performance Considerations

- Columnstore indexes for analytical workloads
- Partitioning strategies for large datasets
- Optimized stored procedures with error handling
- Efficient star schema design for fast queries

## 🧪 Testing

The project includes comprehensive data quality tests:

- **Silver Layer Tests**: Data completeness, format validation, business rule checks
- **Gold Layer Tests**: Referential integrity, aggregation accuracy, dimensional consistency

## 📚 Documentation

Detailed documentation is available in the `documents/` folder:

- **Architecture Diagram**: Visual representation of the system architecture
- **Data Flow Diagram**: End-to-end data processing workflow
- **Data Model**: Dimensional model and table relationships
- **Data Catalog**: Comprehensive data dictionary and metadata


## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏷️ Tags

`data-warehouse` `sql-server` `medallion-architecture` `bronze-silver-gold` `dimensional-modeling` `etl` `business-intelligence` `analytics` `data-engineering`

---

**Note**: Remember to update connection strings, server names, and database names according to your environment before executing the scripts.