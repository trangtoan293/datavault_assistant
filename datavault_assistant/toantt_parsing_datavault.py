from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
from datavault_assistant.core.metadata.source_handler import SourceMetadataProcessor
from datavault_assistant.core.utils.db_handler import DatabaseHandler
from datavault_assistant.configs.settings import ParserConfig
from datavault_assistant.core.utils.llm import init_llm
from datavault_assistant.configs.settings import get_settings
from pathlib import Path
import pandas as pd

settings = get_settings()
db_config = {
    'dbname':settings.DB_NAME,
    'user':settings.DB_USER,
    'password':settings.DB_PASSWORD,
    'host':settings.DB_HOST,
    'port':settings.DB_PORT
}

db = DatabaseHandler(db_config)
config = ParserConfig()
processor = DataProcessor(config)
metadata=pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx")
source_processor = SourceMetadataProcessor( db_handler=db, system_name='FLEXLIVE', user_id='admin' )
source_processor.process_source_metadata(metadata)
analyzer = DataVaultAnalyzer(init_llm(provider="groq"))
result=analyzer.analyze(metadata.to_string(index=False))
processor.process_data(
    input_data=result,
    mapping_data=metadata,
    output_dir=Path("output")
)
