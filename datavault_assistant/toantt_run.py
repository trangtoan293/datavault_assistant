# from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
# from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
# from datavault_assistant.configs.settings import ParserConfig
# from datavault_assistant.core.utils.llm import init_llm
# from pathlib import Path
# import pandas as pd

# config = ParserConfig()
# processor = DataProcessor(config)
# metadata=pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx")
# analyzer = DataVaultAnalyzer(init_llm(provider="groq"))
# result=analyzer.analyze(metadata.to_string(index=False))
# processor.process_data(
#     input_data=result,
#     mapping_data=metadata,
#     output_dir=Path("output")
# )

from datavault_assistant.core.utils.llm import init_llm
from langchain_core.prompts import ChatPromptTemplate
import re 

llm = init_llm(provider="ollama")

def analyze(metadata:str):
    system_template = """
As a Data Vault 2.0 Expert, please assist me with:

1. Business Context:
- [Brief description of business domain and use case]
- [Data integration and historization requirements]
- [Performance and scalability requirements]

2. Source Systems:
- [List of primary source systems]
- [Expected data volume and velocity]
- [Change data capture requirements]

3. Data Vault 2.0 Design:
- Hub entities to be identified
- Link relationships to be mapped
- Satellite tables for descriptive/temporal data
- Raw vault vs Business vault considerations
- Multi-tenant architecture requirements (if applicable)

4. Technical Implementation:
- Loading patterns & automation approach
- Performance optimization strategies
- Data quality framework integration
- Testing methodology

5. Specific Guidance Needed:
- [Specific issues requiring consultation]
- [Technical challenges being faced]
- [Design decisions needing validation]
            
Here are some requirements:
A Hub component represent an unique business object, and a Link component represent a relationship between Hubs.
A component can be derived from multiple source tables.
A Link component must include at least 2 existed Hub components in relationships.
Do NOT assume that a table is a Hub or Link component if it does not meet the requirements.

Think step by step and response the final result with the following JSON format in a markdown cell:
```json
{{
    "hubs": [
        {{
            "name": Hub component name, it should be in the format of HUB_<business_object_name>,
            "business_keys": List of business key columns,
            "source_tables": List of source tables,
            "description": Short description of the component
        }}
    ],
    "links": [
        {{
            "name": Link component name, it should be in the format of LNK_<relationship_name>,
            "related_hubs": List of related hubs,
            "business_keys": List of business key columns, including all bussiness keys from related hubs,
            "source_tables": List of source tables,
            "description": Short description of the component
        }}
    ]
}}
```
""".strip()
    chain = (ChatPromptTemplate.from_messages([
            ("system", system_template),
          
            ("human", "Table Metadata: {metadata}")
        ])
        | llm 
    )
    try:
        analysis = chain.invoke({"metadata": metadata})
        analysis = re.search(r"```(json)?\n(.*)\n```", analysis.content, re.DOTALL).group(2)
        return analysis
        # analysis = 
        # return analysis.content
    except Exception as e:
        print(f"Error analyzing metadata: {str(e)}")
        
        
if __name__ == "__main__":
    import pandas as pd 
    metadata=pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx")
    metadata_str=metadata.to_string(index=False)
    result=analyze(metadata_str)
    print(result)
    # print(analyze(metadata_str))
    
