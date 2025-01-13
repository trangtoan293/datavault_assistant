tạo app để generate data vault 2.0 tự động :
functional requirements:
* read excel file / csv file as input for metadata source - Done core.nodes.metadata_handler
* phân tích metadata source - done core.nodes.data_vault_builder
* đề xuất mô hình data vault 2.0 - done core.nodes.data_vault_builder
* sinh ra yaml file data vault 2.0 theo format - core.nodes.data_vault_parser
* lưu trữ kết quả về database - core.metadata.source_handler core.meatadata.raw_vault_handler
* sinh sql script để tạo bảng - chưa 

Non-functional requirements :
* lưu trữ metadata source 
* lưu trữ kết quả về data base 
* quản lý theo từng project của dự án 
---
progress : 
đang test đề xuất mô hình data vault 2.0 
=> kết quả chưa đc consistency 
---






Tôi muốn tạo một function đọc file metadata của database.
sau đó lưu trữ vào một database hoặc topic kafka # Main API routes
function này nên theo API endpoint như bên dưới 
/api/v1/
  /metadata/
    POST /upload           # Upload metadata files
    GET /sources          # List all metadata sources
    GET /sources/{id}     # Get specific metadata source
    
  /datavault/
    POST /generate        # Generate Data Vault model
    GET /models          # List all generated models
    GET /models/{id}     # Get specific model
    
  /output/
    GET /yaml/{model_id}  # Get YAML output
    GET /sql/{model_id}   # Get SQL scripts
    POST /dbt/sync       # Sync with DBT
