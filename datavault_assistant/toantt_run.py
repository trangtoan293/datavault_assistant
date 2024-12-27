# from api.controllers.metadata_controller import MetadataController
from core.nodes.datavault_analyzer import HubAnalyzer
from core.nodes.metadata_reader import MetadataSourceParser
from core.utils.llm import init_llm

llm=init_llm('ollama')

parser=MetadataSourceParser()
metadata = parser.read_metadata_source(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv")

controller = HubAnalyzer(llm)
result = controller.analyze(metadata)

print(result)
