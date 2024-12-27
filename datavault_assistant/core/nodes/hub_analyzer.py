from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser


class HubAnalyzer:
    def __init__(self,llm):
        self.llm = llm
        self.prompt = ChatPromptTemplate.from_messages([
        ("system", """
You are a Data Vault 2.0 modeling expert.
Analyze the table metadata given by user and recommend appropriate HUB and LINK components.

Response with the following JSON format without any explanation:
[
    {{
        "name": Component name, it should be in the format of HUB_<business_object_name> or LNK_<relationship_name>,
        "type": "HUB" or "LINK", a LINK can not appear without at least 2 existed HUBs in the same source table,
        "business_keys": List of business key columns,
        "source_tables": List of source tables, one component can be derived from multiple source tables,
        "description": Short description of the component
    }}
]
"""),
            ("human", """
Table Metadata: {metadata}
Analyze and classify according to the above format. Ensure all columns are classified with clear reasoning.""")
        ])
        self.chain = self.prompt | self.llm 
        
    def analyze(self,metadata):
        try:
            analysis = self.chain.invoke({"metadata": metadata})
            return analysis.content
        except Exception as e:
            print(f"Error analyzing table: {str(e)}")
            return None

    