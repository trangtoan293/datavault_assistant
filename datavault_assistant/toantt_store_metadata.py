from datavault_assistant.configs.settings import get_settings
from datavault_assistant.core.utils.db_handler import DatabaseHandler
from datavault_assistant.core.metadata.raw_vault_handler import DataVaultMetadataProcessor

settings = get_settings()
db_config = {
    'dbname':settings.DB_NAME,
    'user':settings.DB_USER,
    'password':settings.DB_PASSWORD,
    'host':settings.DB_HOST,
    'port':settings.DB_PORT
}
print('Connecting to database')
db = DatabaseHandler(db_config)
dv_processor = DataVaultMetadataProcessor(db_handler=db, user_id='admin')
dv_processor.process_yaml_files(r'D:\01_work\08_dev\ai_datavault\datavault_assistant\output\sat_corporation_details_metadata.yaml')