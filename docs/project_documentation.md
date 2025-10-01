# BackendApp-Core-C Project Documentation

## Overview

BackendApp-Core-C is an accounting operations processing system designed to handle accounting data from multiple accounting software systems. The application can parse different file formats, extract accounting operations, store them in a standardized format, and provide various processing and reporting capabilities, including audit support features.

## System Architecture

The system follows a modern, layered architecture:

1. **API Layer**: FastAPI-based RESTful API
2. **Service Layer**: Business logic for processing accounting data
3. **Data Access Layer**: SQLAlchemy ORM for database interactions
4. **Storage Layer**: PostgreSQL database and MinIO for file storage

## Core Components

### File Import and Parsing

The system can process accounting files from different sources:

- **Template Detection**: Automatically detects the accounting software format
- **Parsers**: Specialized parsers for different file formats (AJUR, Microinvest, Rival)
- **Normalization**: Converts data to a standardized format

### Accounting Operations Processing

- **Storage**: All operations are stored in a standardized format
- **Filtering**: Comprehensive filtering options for data analysis
- **Reporting**: Generation of account-specific reports with filtering rules
- **Audit Support**: Fields for audit verification, deviation tracking, and control actions

### API

- **Authentication**: JWT-based authentication system
- **Operations API**: CRUD operations for accounting data
- **File API**: Upload, download, and processing of accounting files
- **System API**: System information and configuration endpoints

### Storage

- **Database**: PostgreSQL for structured data
- **Object Storage**: MinIO for file storage
- **Migrations**: Alembic for database schema management

## Data Flow

1. **File Upload**: User uploads an accounting file
2. **Template Detection**: System identifies the accounting software format
3. **Parsing**: The appropriate parser extracts operations from the file
4. **Normalization**: Data is converted to a standardized format
5. **Storage**: Operations are stored in the database
6. **Processing**: Additional processing like account grouping and filtering
7. **Reporting**: Generation of specialized reports
8. **Audit**: Support for auditing processes with specialized fields

## Database Schema

### Main Entities

#### AccountingOperation

The core entity representing an accounting operation with these key fields:

- **Basic Information**: operation_date, amount, description
- **Account Information**: debit_account, credit_account, analytical_debit, analytical_credit
- **Document Information**: document_type, document_number
- **Partner Information**: partner_name
- **Audit Information**: 
  - sequence_number: Order number in the accounting journal
  - verified_amount: Amount verified during audit
  - deviation_amount: Difference between original and verified amounts
  - control_action: Actions taken during audit
  - deviation_note: Additional notes about deviations
- **Metadata**: template_type, created_at, import_uuid

#### UploadedFile

Represents files uploaded to the system:

- **Basic Information**: filename, content_type, size
- **Storage Information**: s3_key
- **Processing Information**: processed, template_type
- **Metadata**: upload_time, import_uuid

#### User

User accounts for authentication and authorization.

## API Endpoints

### Authentication

- `POST /auth/token` - Obtain an access token
- `POST /auth/refresh` - Refresh an expired token
- `GET /auth/me` - Get current user information

### Files

- `POST /files/upload` - Upload a new accounting file
- `GET /files` - List uploaded files
- `GET /files/{file_id}` - Get file details
- `GET /files/{file_id}/download` - Download a file
- `POST /files/process/{file_id}` - Process a file to extract operations

### Operations

- `GET /operations` - List operations with filtering
- `GET /operations/{operation_id}` - Get operation details
- `GET /operations/statistics/summary` - Get operation statistics
- `GET /operations/export` - Export operations
- `POST /operations/process-import/{import_uuid}` - Process import batch

### System

- `GET /system/info` - Get system information
- `GET /system/health` - Check system health

## Deployment

### Requirements

- Python 3.8+
- PostgreSQL 12+
- MinIO or S3-compatible storage
- Docker and Docker Compose (optional)

### Local Development

1. Clone the repository
2. Set up a virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Configure environment variables in `.env`
5. Run database migrations: `alembic upgrade head`
6. Start the development server: `uvicorn main:app --reload`

### Docker Deployment

1. Build the Docker image: `docker-compose build`
2. Start the services: `docker-compose up -d`
3. Run migrations: `docker-compose exec app alembic upgrade head`

### Production Deployment

For production deployments, we recommend:

1. Using Kubernetes for orchestration
2. Implementing proper SSL/TLS termination
3. Setting up proper monitoring and logging
4. Configuring database backups
5. Using a production-ready PostgreSQL setup with proper replication

## Data Export Format

The system now supports exporting accounting operations in the following standardized format:

| № по ред | Вид документ | Документ № | Дата | Дт с/ка | Аналитична сметка/Партньор (Дт) | Кт с/ка | Аналитична сметка/Партньор (Кт) | Сума | Обяснение/Обоснование | Установена сума при одита | Отклонение | Установено контролно действие при одита | Отклонение (забележка) |
|----------|--------------|------------|------|---------|--------------------------------|---------|----------------------------------|------|-------------------------|----------------------------|------------|----------------------------------------|------------------------|
| 1        | Фактура      | 1001       | 2023-01-15 | 411 | Клиент X | 702 | Услуга Y | 1000.00 | Продажба на услуги | 1000.00 | 0.00 | Проверени документи | - |

This format is used for both the database structure and the export files, ensuring consistency between the stored data and the exported reports.