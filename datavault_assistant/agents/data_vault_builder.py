
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_ollama import ChatOllama

llm = ChatOllama(
    base_url='http://192.168.1.8:11434',
    model='llama3.1:8b-instruct-q8_0',
    temperature=0,
    num_ctx=10000
)

class LLMDataVaultBuilder:
    def __init__(self, llm):
        self.llm = llm
        self.prompt = ChatPromptTemplate.from_messages([
        ("system", """
You are a Data Vault 2.0 modeling expert. Analyze the table metadata and recommend appropriate 
Data Vault components (HUB, LINK, or SATELLITE) on Oracle database.
You must utilize all columns in the table metadata to make a recommendation. 
For each table, you MUST classify it as one of:
1. HUB: Contains business keys that identify unique business objects
2. LINK: Represents relationships between HUBs, and contains foreign keys to HUBs. There is no link table connect from hub table to satellite table, Satellite table connect directly to hub table via dv_hkey.
3. SATELLITE: Contains descriptive attributes for HUBs or LINKs
4. One table can be classified as multiple components if necessary (e.g., a table can be both a HUB , LINK, or SATELLITE). It depends on the context of the data.

Follow Data Vault 2.0 best practices and standards.
You should name target tables with the following format:
- HUB: HUB_<business_object_name>
- LINK: LNK_<relationship_name>
- SATELLITE: SAT_<hub_name>_<description>
- SATELLITE_LINK: LSAT_<link_name>_<description>
Link table must be named with the relationship name in plural form (e.g., LNK_customer_orders) and including at least 2 existed hub tables in relationships.
Satellite table is never contain business keys and always contain descriptive attributes. It should be navigated from HUB or LINK table.

Before making a recommendation, you should consider the following:
- You must utilize all columns in the table metadata to make a recommendation. 
- Based on column name or description, you can identify business keys, relationships, and descriptive attributes.
- Thanks to descriptive attributes, you can clasify to SATELLITE table as a topic that describe information of Hub or Link table.
- If a column is not clear or ambiguous, you can put it in the descriptive attributes of the SATELLITE table.
Always response with the following JSON format without any explanation:
{{
    "table_name": Original table name,
    "recommended_component": List of recommended components [
        {{
            "target_table": Recommended table name,
            "component_type": "HUB", "LINK", or "SATELLITE",
            "business_key": List of business key columns for HUBs only (e.g., ["customer_id", "customer_name"]). Never apply for LINK and SATELLITE table.
            "hash_key" : Hash key columns that derived from business key columns for Hub or Link. The format is DV_HKEY_<hub_target_table_name> for hub or DV_HKEY_<link_target_table_name> for link.
            "relationships": List of related HUBs, including {{table_name, relationship_type, relationship_hkey ( derived from hub or link table ))}}.
            "descriptive_attrs": List of descriptive attributes for SATELLITEs including {{column_name, data_type, description}}. Never include business keys and never apply for HUB and LINK table.
        }}
    ],
    "Explanation": Explanation for the recommendation. Give me specific reasons why you recommend the business entity to mapping to a HUB, LINK, or SATELLITE.
    "Implementation_notes": List of specific implementation considerations
}}
"""),
            ("human", """
Table Metadata: {table_metadata}
Analyze this table and provide a complete Data Vault 2.0 component recommendation.""")
        ])

        self.chain = self.prompt | self.llm # | JsonOutputParser()
        
    def recommend_data_model(self, table_metadata):
        try:
            analysis = self.chain.invoke({"table_metadata": table_metadata})
            return analysis
        except Exception as e:
            print(f"Error analyzing table: {str(e)}")
            return None

if __name__ == "__main__":
    analyzer = LLMDataVaultBuilder(llm)
    table_metadata = [
        {
            "table_name": "customer",
            "columns": ["customer_id", "customer_name", "customer_email"]
        },
        {
            "table_name": "order",
            "columns": ["order_id", "order_date", "customer_id"]
        }
    ]
    analysis = analyzer.analyze_table(table_metadata)
    analysis.pretty_print()

    