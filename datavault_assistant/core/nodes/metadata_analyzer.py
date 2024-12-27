
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_ollama import ChatOllama

class LLMMetadataAnalyzer:
    def __init__(self,llm):
        self.llm = llm
        self.prompt = ChatPromptTemplate.from_messages([
        ("system", """
You are a professional Data Architect. 
Your task is to analyze database metadata and classify columns into business concepts/topics, following Data Vault 2.0 principles.

BUSINESS RULES:
1. Each column must be classified into a specific business concept
2. Business concepts should accurately reflect the business domain
3. Columns related to identifiers should be marked as potential Hubs
4. Columns representing relationships should be marked as potential Links
5. Columns containing descriptive/contextual data should be marked as potential Satellites
6. ONLY USE the provided metadata and descriptions for classification (do not infer additional information).
7. One table should separate into maximum 3 business concepts.

Response with following JSON format:
{{
    "TABLE_NAME": original Table name,
    "COLUMN_NAME": list of Column name related to business concept,
    "BUSINESS_CONCEPT": Business concept
}}
"""),
            ("human", """
Table Metadata: {table_metadata}
Analyze and classify according to the above format. Ensure all columns are classified with clear reasoning.""")
        ])

        self.chain = self.prompt | self.llm | JsonOutputParser()
        
    def analyze_table(self,table_metadata):
        try:
            analysis = self.chain.invoke({"table_metadata": table_metadata})
            return analysis
        except Exception as e:
            print(f"Error analyzing table: {str(e)}")
            return None

    