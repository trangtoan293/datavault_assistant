from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
from datavault_assistant.core.metadata.source_handler import SourceMetadataProcessor
from datavault_assistant.core.utils.db_handler import DatabaseHandler
from datavault_assistant.configs.settings import ParserConfig
from datavault_assistant.core.utils.llm import init_llm
from datavault_assistant.configs.settings import get_settings
from pathlib import Path
import json
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
# result=analyzer.analyze(metadata.to_string(index=False))
result = json.loads("""{"hubs":[{"name":"HUB_CUSTOMER","business_keys":["CUS_CUSTOMER_CODE","CB_CUS_ID"],"source_tables":["COREBANK_CUSTOMER","CORECARD_CUSTOMER"],"description":"Customer information"},{"name":"HUB_CORPORATION","business_keys":["CUS_CORP_CODE"],"source_tables":["COREBANK_CORP"],"description":"Corporation information"}],"links":[{"name":"LNK_CUSTOMER_CORPORATION","related_hubs":["HUB_CUSTOMER","HUB_CORPORATION"],"business_keys":["CUS_CUSTOMER_CODE","CB_CUS_ID","CUS_CORP_CODE"],"source_tables":["COREBANK_CUSTOMER","CORECARD_CUSTOMER","COREBANK_CORP"],"description":"Relationship between customer and corporation"}],"satellites":[{"name":"SAT_CUSTOMER_DESCRIPTION","hub":"HUB_CUSTOMER","business_keys":["CUS_CUSTOMER_CODE","CB_CUS_ID"],"source_table":"COREBANK_CUSTOMER","descriptive_attrs":["CUS_NAME_1","CUS_GENDER","CUS_BIRTH_INCORP_DATE","CUS_NATIONALITY","CUS_STREET","CUS_ADDRESS","CUS_MOBILE_NUMBER","CUS_EMAIL_1","CUS_LEGAL_ID","CUS_LEGAL_DOC_NAME","CUS_LEGAL_ISS_DATE","CUS_LEGAL_ISS_AUTH","CUS_POSITION","CUS_SECTOR","CUS_CREATION_DATE","BRN_CODE"]},{"name":"SAT_CUSTOMER_CARD_DESCRIPTION","hub":"HUB_CUSTOMER","business_keys":["CUS_CUSTOMER_CODE","CB_CUS_ID"],"source_table":"CORECARD_CUSTOMER","descriptive_attrs":["CB_CIF_NO","CB_CUSTOMER_IDNO","CB_SEX","CB_DOB","CB_NATIONALITY","CB_MOBILE_NO","CB_EMAIL","CB_USER2_DATE_1","CB_OCCUPN","CB_CARDHOLDER_NAME","CB_ID_TYPE","CB_CREATION_DATE","BRN_CODE"]},{"name":"SAT_CORPORATION_DESCRIPTION","hub":"HUB_CORPORATION","business_keys":["CUS_CORP_CODE"],"source_table":"COREBANK_CORP","descriptive_attrs":["CUS_NAME","CUS_TAX_CODE","CUS_TAX_DATE","CUS_CORP_ID","CUS_CORP_DATE","CUS_ADDRESS","BRN_CODE"]}],"link_satellites":[{"name":"LSAT_CUSTOMER_CORPORATION_DESCRIPTION","link":"LNK_CUSTOMER_CORPORATION","business_keys":["CUS_CUSTOMER_CODE","CB_CUS_ID","CUS_CORP_CODE"],"source_table":"COREBANK_CUSTOMER","descriptive_attrs":["CUS_NAME_1","CUS_GENDER","CUS_BIRTH_INCORP_DATE","CUS_NATIONALITY","CUS_STREET","CUS_ADDRESS","CUS_MOBILE_NUMBER","CUS_EMAIL_1","CUS_LEGAL_ID","CUS_LEGAL_DOC_NAME","CUS_LEGAL_ISS_DATE","CUS_LEGAL_ISS_AUTH","CUS_POSITION","CUS_SECTOR","CUS_CREATION_DATE","BRN_CODE"]}]}
""")
results=processor.process_data(
    input_data=result,
    mapping_data=metadata,
    output_dir=Path("output")
)
