# from api.controllers.metadata_controller import MetadataController
from core.nodes.metadata_handler import MetadataHandler
from core.utils.llm import init_llm
from pathlib import Path
llm=init_llm('groq')
path=r'D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv'
handler=MetadataHandler(llm)
metadata= handler.read_metadata_source(Path(path))
result= handler.analyze_local_file(Path(path))
print(result)



