from langchain_core.prompts import ChatPromptTemplate
import pandas as pd 
import re
from pathlib import Path
from pydantic import BaseModel
import logging
from typing import List, Dict,Optional
from dataclasses import dataclass
import json
from typing import Any, Dict, List, Optional
from datavault_assistant.core.prompt.datavault_analyze_template import hub_lnk_analyze_prompt_template,sat_analyze_prompt_template
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datavault_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DataVaultValidationError(Exception):
    message: str
    
class AnalyzerState(BaseModel):
    metadata_content: Optional[str] = None
    hub_analysis: Optional[str] = None
    sat_analysis: Optional[str] = None
    final_analysis: Optional[str] = None
    metadata_result: Optional[Dict[str, Any]] = None
    warnings: List[str] = []
    
    @property
    def hubs_metadata(self):
        return {hub['name']: hub for hub in self.metadata_result.get('hubs', [])} if self.metadata_result else {}

    @property
    def links_metadata(self):
        return {link['name']: link for link in self.metadata_result.get('links', [])} if self.metadata_result else {}

    @property
    def sats_metadata(self):
        return {sat['name']: sat for sat in self.metadata_result.get('satellites', [])} if self.metadata_result else {}

    @property
    def lsats_metadata(self):
        return {lsat['name']: lsat for lsat in self.metadata_result.get('link_satellites', [])} if self.metadata_result else {}

class DataVaultAnalyzer:
    def __init__(self,llm,metadata_content:Optional[str]=None,metadata_result:Optional[Dict[str, Any]]=None):
        self.llm = llm
        self.state=AnalyzerState()
        self.hub_analyzer=HubAnalyzer(llm)
        self.sat_analyzer=SatelliteAnalyzer(llm)
        self.state.metadata_content = metadata_content
        self.metadata_result = self.state.metadata_result 
        self.warnings = self.state.warnings
        # Store validated metadata for cross-reference
        self.hubs_metadata = self.state.hubs_metadata
        self.links_metadata = self.state.links_metadata
        self.sats_metadata = self.state.sats_metadata
        self.lsats_metadata = self.state.lsats_metadata

    
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
            tmp = json.loads(self.state.hub_analysis)
            tmp.update(json.loads(self.state.sat_analysis))
            self.state.final_analysis = tmp
            return self.state.final_analysis
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            raise e

    def validate_all(self, metadata_result=None) -> List[str]:
        """Validate all components in the metadata"""
        try:
            result = metadata_result if metadata_result else self.metadata_result
            
            # Validate Hubs
            for hub in result.get('hubs', []):
                self.warnings.extend(self.validate_hub(hub))

            # Validate Links  
            for link in result.get('links', []):
                self.warnings.extend(self.validate_link(link))

            # Validate Satellites
            for sat in result.get('satellites', []):
                self.warnings.extend(self.validate_sat(sat))

            # Validate Link Satellites
            for lsat in result.get('link_satellites', []):
                self.warnings.extend(self.validate_sat_link(lsat))

            return self.warnings

        except DataVaultValidationError as e:
            self.warnings.append(f"Validation Error: {str(e)}")
            return self.warnings

    def validate_hub(self, hub_data: Dict[str, Any]) -> List[str]:
        """Validate hub metadata"""
        local_warnings = []
        required_fields = ['name', 'business_keys', 'source_tables', 'description']
        
        # Check required fields
        for field in required_fields:
            if field not in hub_data:
                raise DataVaultValidationError(f"Missing required field in hub: {field}")
        
        # Validate hub name format
        if not hub_data['name'].startswith('HUB_'):
            local_warnings.append(f"Hub name should start with 'HUB_': {hub_data['name']}")
        
        # Validate business keys
        if not hub_data['business_keys']:
            raise DataVaultValidationError(f"Business keys cannot be empty for hub: {hub_data['name']}")
            
        # Validate source tables
        if not hub_data['source_tables']:
            raise DataVaultValidationError(f"Source tables cannot be empty for hub: {hub_data['name']}")
            
        return local_warnings

    def validate_link(self, link_data: Dict[str, Any]) -> List[str]:
        """Validate link metadata"""
        local_warnings = []
        link_keys = set(link_data["business_keys"])
        link_name = link_data["name"]
        
        # Validate link name format
        if not link_name.startswith('LNK_'):
            local_warnings.append(f"Link name should start with 'LNK_': {link_name}")
        
        # Validate related hubs
        if len(link_data["related_hubs"]) < 2:
            raise DataVaultValidationError(
                f"Link {link_name} must have at least 2 related hubs"
            )
        
        # Collect all hub business keys
        all_hub_keys = set()
        for hub_name in link_data["related_hubs"]:
            if hub_name not in self.hubs_metadata:
                raise DataVaultValidationError(
                    f"Link {link_name} references non-existent hub: {hub_name}"
                )
                
            hub_keys = set(self.hubs_metadata[hub_name]["business_keys"])
            all_hub_keys.update(hub_keys)
            
            # Validate that link contains all business keys from each hub
            missing_keys = hub_keys - link_keys
            if missing_keys:
                raise DataVaultValidationError(
                    f"Link {link_name} is missing business keys from hub {hub_name}: {sorted(list(missing_keys))}"
                )
        
        # Check extra keys
        extra_keys = link_keys - all_hub_keys
        if extra_keys:
            raise DataVaultValidationError(
                f"Link {link_name} contains business keys that don't belong to any related hub: {sorted(list(extra_keys))}"
            )
        
        return local_warnings

    def validate_sat(self, sat_data: Dict[str, Any]) -> List[str]:
        """Validate satellite metadata"""
        local_warnings = []
        required_fields = ["name", "hub", "source_table", "business_keys", "descriptive_attrs"]
        
        # Check required fields
        for field in required_fields:
            if field not in sat_data:
                local_warnings.append(f"Missing required field in satellite: {field}")
        
        if local_warnings:  # Return if missing required fields
            return local_warnings
        
        # Validate satellite name format
        if not sat_data["name"].startswith("SAT_"):
            local_warnings.append(f"Satellite name should start with 'SAT_': {sat_data['name']}")
        
        # Validate hub existence and business keys
        hub_name = sat_data["hub"]
        if hub_name not in self.hubs_metadata:
            raise DataVaultValidationError(
                f"Satellite {sat_data['name']} references non-existent hub: {hub_name}"
            )
        
        # Validate business keys match hub
        hub_keys = set(self.hubs_metadata[hub_name]["business_keys"])
        sat_keys = set(sat_data["business_keys"])
        
        if hub_keys != sat_keys:
            local_warnings.append(
                f"Satellite {sat_data['name']} business keys don't match hub {hub_name} keys"
            )
            
        return local_warnings

    def validate_sat_link(self, lsat_data: Dict[str, Any]) -> List[str]:
        """Validate link satellite metadata"""
        local_warnings = []
        link_name = lsat_data["link"]
        
        # Validate link satellite name format
        if not lsat_data["name"].startswith("LSAT_"):
            local_warnings.append(f"Link satellite name should start with 'LSAT_': {lsat_data['name']}")
        
        # Validate link existence
        if link_name not in self.links_metadata:
            raise DataVaultValidationError(
                f"Link satellite references non-existent link: {link_name}"
            )
        
        # Validate business keys consistency
        link_keys = set(self.links_metadata[link_name]["business_keys"])
        lsat_keys = set(lsat_data["business_keys"])
        
        # Check missing keys
        missing_keys = link_keys - lsat_keys
        if missing_keys:
            local_warnings.append(
                f"Link satellite {lsat_data['name']} is missing business keys from parent link {link_name}: {sorted(list(missing_keys))}"
            )
        
        # Check extra keys
        extra_keys = lsat_keys - link_keys
        if extra_keys:
            local_warnings.append(
                f"Link satellite {lsat_data['name']} contains extra business keys not in parent link {link_name}: {sorted(list(extra_keys))}"
            )
            
        return local_warnings
    def get_result(self):
        return self.state.final_analysis
    
class HubAnalyzer:
    def __init__(self,llm):
        self.llm = llm

    def analyze(self,metadata:str):
        system_template = hub_lnk_analyze_prompt_template
        chain = (ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", "Table Metadata: {metadata}")
            ])
            | self.llm 
        )
        try:
            analysis = chain.invoke({"metadata": metadata})
            logger.info(f"Hub/Link analysis content: {analysis.content}")
            analysis = re.search(r".*```(json)?\n(.*)\n```", analysis.content, re.DOTALL).group(2)
            logger.info(f"Hub/Link analysis: {analysis}")
            return analysis
        except Exception as e:
            print(f"Error analyzing metadata: {str(e)}")
            
class SatelliteAnalyzer:
    def __init__(self,llm):
        self.llm = llm
        
    def analyze(self,metadata:str,hub_analysis:str):
        system_template = sat_analyze_prompt_template

        chain = (
            ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", "Table Metadata: {metadata}\nHUB and LINK Analysis: {hub_analysis}")
            ])
            | self.llm
        )

        try:
            analysis = chain.invoke({"metadata": metadata, "hub_analysis": hub_analysis})
            logger.info(f"Satellite analysis content: {analysis.content}")
            analysis = re.search(r".*```(json)?\n(.*)\n```", analysis.content, re.DOTALL).group(2)
            logger.info(f"Satellite analysis: {analysis}")
            return analysis
        except Exception as e:
            print(f"Error analyzing metadata: {str(e)}")
            # return state
        
if __name__ == "__main__":
    from datavault_assistant.core.utils.llm import init_llm
    llm = init_llm(provider="groq")
    metadata=pd.read_excel(r"D:\01_work\08_dev\ai_datavault\datavault_assistant\datavault_assistant\data\test_dv_autovault.xlsx")
    metadata=metadata.to_string(index=False)
    analyzer = DataVaultAnalyzer(llm)
    result=analyzer.analyze(metadata)
    warnings = analyzer.validate_all(result)
    if warnings:
        print(warnings)
    else:
        print("Validation passed successfully!")