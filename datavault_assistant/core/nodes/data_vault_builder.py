from langchain_core.prompts import ChatPromptTemplate
import pandas as pd 
import re
from pathlib import Path
from langchain_groq import ChatGroq
from pydantic import BaseModel
import logging
from typing import List, Dict,Optional
from abc import ABC, abstractmethod
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datavault_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AnalyzerState(BaseModel):
    metadata_content: str
    hub_analysis: str
    sat_analysis: str
    final_analysis: str

class DataVaultAnalyzer:
    def __init__(self,llm,metadata_content:Optional[str]=None):
        self.llm = llm
        self.state=AnalyzerState
        self.hub_analyzer=HubAnalyzer(llm)
        self.sat_analyzer=SatelliteAnalyzer(llm)
        self.state.metadata_content = metadata_content
        
    def get_metadata(self,path:Path):
        df = pd.read_csv(path)
        metadata_content = df.to_string(index=False)
        self.state.metadata_content=metadata_content
        return metadata_content
        
    def analyze(self,metadata_content):
        self.state.metadata_content=metadata_content
        try:
            if not self.state.metadata_content:
                raise ValueError("No metadata loaded. Call get_metadata first.")

            # Perform hub analysis
            self.state.hub_analysis = self.hub_analyzer.analyze(self.state.metadata_content)
            
            # Perform satellite analysis
            self.state.sat_analysis = self.sat_analyzer.analyze(
                metadata=self.state.metadata_content,
                hub_analysis=self.state.hub_analysis
            )
            
            # Combine analyses
            self.state.final_analysis = json.dumps({
                "hub_link_analysis": (self.state.hub_analysis),
                "satellite_analysis": (self.state.sat_analysis)
            }, indent=2)
            
            return self.state.final_analysis
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            raise e
        # self.state.hub_analysis=self.hub_analyzer.analyze(self.state.metadata_content)
        # self.state.sat_analysis=self.sat_analyzer.analyze(metadata=self.state.metadata_content,hub_analysis=self.state.hub_analysis)
        # self.state.final_analysis = self.state.hub_analysis + self.state.sat_analysis
        # return self.state.final_analysis
    
    def get_result(self):
        return self.state.final_analysis
    
class HubAnalyzer:
    def __init__(self,llm):
        self.llm = llm

    def analyze(self,metadata:str):
        system_template = """
    You are a Data Vault 2.0 modeling expert in banking domain.
    Analyze the table metadata given by user and recommend appropriate HUB and LINK components.
                
    Here are some requirements:
    A Hub component represent an unique business object, and a Link component represent a relationship between Hubs.
    A component can be derived from multiple source tables.
    A Link component must include at least 2 existed Hub components in relationships.
    Do NOT assume that a table is a Hub or Link component if it does not meet the requirements.

    Think step by step and response the final result with the following JSON format in a markdown cell:
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
    """.strip()
        chain = (ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", "Table Metadata: {metadata}")
            ])
            | self.llm 
        )
        try:
            analysis = chain.invoke({"metadata": metadata})
            analysis = re.search(r"```(json)?\n(.*)\n```", analysis.content, re.DOTALL).group(2)
            # analysis = 
            return analysis
        except Exception as e:
            print(f"Error analyzing metadata: {str(e)}")
            
class SatelliteAnalyzer:
    def __init__(self,llm):
        self.llm = llm
        
    def analyze(self,metadata:str,hub_analysis:str):
        system_template = """
    You are a Data Vault 2.0 modeling expert in banking domain.
    Analyze the table metadata and an analysis of HUB and LINK components given by user \
    and recommend appropriate SATELLITE components.

    Here are some requirements:
    A Satellite component contains descriptive attributes for HUBs or LINKs, but not include business keys.
    A Satellite component should be navigated from a HUB or LINK component.
    Each column in the source table must not be duplicated in multiple components, and no column should be left out.

    Think step by step and response the final result with the following JSON format in a markdown cell:
    {{
        "satellites": [
            {{
                "name": Satellite component name, it should be in the format of SAT_<business_object_name>_<description>,
                "hub": Related Hub component name,
                "business_keys": List of business key columns from related Hub component,
                "source_table": Source table name,
                "descriptive_attrs": List of descriptive attributes
            }}
        ],
        "link_satellites": [
            {{
                "name": Satellite component name, it should be in the format of LSAT_<relationship_name>_<description>,
                "link": Related Link component name,
                "business_keys": List of business key columns from related Link component,
                "source_table": Source table name,
                "descriptive_attrs": List of descriptive attributes
            }}
    }}
    """.strip()

        chain = (
            ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", "Table Metadata: {metadata}\nHUB and LINK Analysis: {hub_analysis}")
            ])
            | self.llm
        )

        try:
            analysis = chain.invoke({"metadata": metadata, "hub_analysis": hub_analysis})
            analysis = re.search(r"```(json)?\n(.*)\n```", analysis.content, re.DOTALL).group(2)
            return analysis
        except Exception as e:
            print(f"Error analyzing metadata: {str(e)}")
            # return state
        
if __name__ == "__main__":
    llm = ChatGroq( 
                model="llama-3.3-70b-versatile" ,
                api_key="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",
                temperature=0,
            )
            
    metadata=pd.read_csv(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\metadata_src.csv")
    metadata=metadata.to_string(index=False)
    analyzer = DataVaultAnalyzer(llm)
    result=analyzer.analyze(metadata)
    print(result)