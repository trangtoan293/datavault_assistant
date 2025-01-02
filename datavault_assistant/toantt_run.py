# from api.controllers.metadata_controller import MetadataController
from core.nodes.data_vault_parser import DataProcessor
from configs.settings import ParserConfig
from pathlib import Path
import json
import pandas as pd

config = ParserConfig()
processor = DataProcessor(config)

input_file=r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\sat_lsat.json"
output_dir=Path("output")
mapping_file=Path(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv")

with open(input_file, 'r', encoding='utf-8') as f:
    input_data= json.load(f)
mapping_data = pd.read_csv(mapping_file)


results = processor.process_data(
    input_data=input_data,
    mapping_data=mapping_data,
    output_dir=Path("output")
)

print(results)