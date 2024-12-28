from core.nodes.metadata_reader import MetadataSourceParser
from core.nodes.metadata_analyzer import LLMMetadataAnalyzer
from core.nodes.data_vault_builder import LLMDataVaultBuilder
from core.utils.llm import init_llm
import json 
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting the Data Vault Assistant")
start_time_app = time.time()
start_time = time.time()

llm = init_llm('ollama')
logger.info("Initialized LLM")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")

start_time = time.time()

parser = MetadataSourceParser()
logger.info("Created MetadataSourceParser instance")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")

start_time = time.time()

metadata = parser.read_metadata_source(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv")
logger.info("Read metadata source")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")

start_time = time.time()
print(metadata)

analyzer = LLMMetadataAnalyzer(llm)
logger.info("Created LLMMetadataAnalyzer instance")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")

start_time = time.time()

analysis = analyzer.analyze_table(metadata)
logger.info("Analyzed metadata")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")
start_time = time.time()

builder = LLMDataVaultBuilder(llm)
logger.info("Created LLMDataVaultBuilder instance")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")
combined = metadata | analysis # {"analyze_result":analysis.content}
start_time = time.time()

result = builder.recommend_data_model(combined)
logger.info("Recommended data model")
logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")

start_time = time.time()

result.pretty_print()
logger.info("Printed result")
logger.info(f"Time taken of application: {time.time() - start_time_app:.2f} seconds")
