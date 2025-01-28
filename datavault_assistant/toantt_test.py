from datavault_assistant.core.nodes.data_vault_builder_v2 import analyze_metadata
from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
from datavault_assistant.configs.settings import ParserConfig
from datavault_assistant.core.utils.llm import init_llm
from datavault_assistant.configs.settings import get_settings
import json
from pathlib import Path

if __name__ == "__main__":  
    from datavault_assistant.core.utils.llm import init_llm
    import pandas as pd
    
    # Khởi tạo LLM
    llm = init_llm(provider="groq")
    
    # Đọc metadata
    metadata = pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx").to_string(index=False)
    
    # Phân tích
    try:
        analysis, warnings = analyze_metadata(metadata, llm)
        config = ParserConfig()
        processor = DataProcessor(config)
        # In kết quả
        print("\nAnalysis Results:")
        print(f"- Hubs: {len(analysis.hubs)}")
        print(f"- Links: {len(analysis.links)}")
        print(f"- Satellites: {len(analysis.satellites)}")
        print(f"- Link Satellites: {len(analysis.link_satellites)}")

        response=analysis.model_dump()

        results=processor.process_data(
                input_data=response,
                mapping_data=metadata,
                output_dir=Path("output")
        )
        
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"- {warning}")
        else:
            print("\nNo warnings - all relationships valid!")
            
    except Exception as e:
        print(f"Analysis failed: {str(e)}")