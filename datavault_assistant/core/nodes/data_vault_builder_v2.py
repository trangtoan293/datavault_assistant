from langchain_core.prompts import ChatPromptTemplate,SystemMessagePromptTemplate,HumanMessagePromptTemplate
import json
from typing import Tuple,List
import re
from datavault_assistant.core.utils.log_handler import create_logger
from datavault_assistant.core.prompt.datavault_analyze_template import hub_lnk_analyze_prompt_template, sat_analyze_prompt_template
from datavault_assistant.core.models.schema import HubLinkAnalysis, SatelliteAnalysis, DataVaultAnalysis
from pydantic import BaseModel
logger = create_logger(name=__name__, log_file="datavault_analyzer_v2.log")


class BaseAnalyzer:
    """
    Base class cho các analyzer, cung cấp các functionality chung cho việc 
    kết hợp structured output với prompt template
    """
    def __init__(self, llm, ):
        self.llm = llm


    def create_structured_chain(self, system_template: str, human_template: str,output_model: BaseModel):
        """
        Tạo chain với structured output, đảm bảo không có conflict trong template variables
        """
        # Tạo các message prompts riêng biệt
        system_message = SystemMessagePromptTemplate.from_template(system_template)
        human_message = HumanMessagePromptTemplate.from_template(human_template)
        
        # Kết hợp thành chat prompt template
        chat_prompt = ChatPromptTemplate.from_messages([
            system_message,
            human_message
        ])
        
        # Augment LLM với structured output
        structured_llm = self.llm.with_structured_output(output_model)
        
        # Tạo chain
        return chat_prompt | structured_llm


class HubLinkAnalyzer(BaseAnalyzer):
    """
    Analyzer cho Hub và Link components với structured output
    """
    def __init__(self, llm):
        super().__init__(llm)
        
        # Tạo chain với structured output
        self.chain = self.create_structured_chain(
            system_template=hub_lnk_analyze_prompt_template,
            human_template="Table Metadata: {metadata}",
            output_model=HubLinkAnalysis
        )

    def analyze(self, metadata: str) -> HubLinkAnalysis:
        """
        Thực hiện phân tích với structured output
        """
        try:
            # Invoke chain sẽ trả về object của class HubLinkAnalysis
            result = self.chain.invoke({"metadata": metadata})
            
            # Log kết quả
            logger.info(f"Successfully analyzed Hub/Link components:")
            logger.info(f"- Found {len(result.hubs)} hubs")
            # logger.info(f"  Hubs: {[hub.name for hub in result.hubs]}")
            logger.info(f"- Found {len(result.links)} links")
            # logger.info(f"  Links: {[link.name for link in result.links]}")
            
            return result
            
        except Exception as e:
            logger.error(f"Hub/Link analysis failed: {str(e)}")
            raise

class SatelliteAnalyzer(BaseAnalyzer):
    """
    Analyzer cho Satellite components với structured output
    """
    def __init__(self, llm):
        super().__init__(llm)
        
        # Tạo chain với structured output
        self.chain = self.create_structured_chain(
            system_template=sat_analyze_prompt_template,
            human_template="Table Metadata: {metadata}\nHUB and LINK Analysis: {hub_analysis}",
            output_model=SatelliteAnalysis
        )

    def analyze(self, metadata: str, hub_link_analysis: HubLinkAnalysis) -> SatelliteAnalysis:
        try:
            # Convert hub_link_analysis to JSON
            hub_analysis_json = hub_link_analysis.model_dump_json()
            
            # Invoke chain với structured output
            result = self.chain.invoke({
                "metadata": metadata,
                "hub_analysis": hub_analysis_json
            })
            analysis_dict= json.loads(result.model_dump_json())
            # Thêm hub analysis vào kết quả để validate
            analysis_dict['hub_analysis'] = json.loads(hub_analysis_json)
            # Create SatelliteAnalysis instance với validation
            result = SatelliteAnalysis.model_validate(analysis_dict)
            
            if result.warnings:
                logger.warning("Satellite analysis completed with warnings:")
                for warning in result.warnings:
                    logger.warning(f"- {warning}")
                    
                # Log chi tiết về các thay đổi tự động
                logger.info("Automatic fixes applied:")
                for sat in result.satellites:
                    logger.info(f"- {sat.name}: {len(sat.business_keys)} business keys, "
                              f"{len(sat.descriptive_attrs)} descriptive attributes")
                    
                logger.info(f"Successfully analyzed Satellite components:")
                logger.info(f"- Found {len(result.satellites)} satellites")
                logger.info(f"- Found {len(result.link_satellites)} link satellites")
            return result

            
        except Exception as e:
            logger.error(f"Satellite analysis failed: {str(e)}")
            raise

class DataVaultAnalyzer:
    """
    Main orchestrator với structured output validation
    """
    def __init__(self, llm):
        self.hub_link_analyzer = HubLinkAnalyzer(llm)
        self.satellite_analyzer = SatelliteAnalyzer(llm)
        
    def analyze(self, metadata: str) -> DataVaultAnalysis:
        try:
            # Step 1: Hub/Link Analysis
            logger.info("Starting Hub/Link analysis...")
            hub_link_result = self.hub_link_analyzer.analyze(metadata)
            
            # Step 2: Satellite Analysis
            logger.info("Starting Satellite analysis...")
            satellite_result = self.satellite_analyzer.analyze(
                metadata,
                hub_link_result
            )
            
            logger.info("Combining the results ...")
            # Step 3: Combine results
            final_result = DataVaultAnalysis(
                hubs=hub_link_result.hubs,
                links=hub_link_result.links,
                satellites=satellite_result.satellites,
                link_satellites=satellite_result.link_satellites
            )
            
            logger.info("Validating final result ...")
            # Validate final result
            self.validate_final_result(final_result)
            
            return final_result
            
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            raise
        
    def validate_relationships(self, analysis: DataVaultAnalysis) -> List[str]:
        """Validate mối quan hệ giữa các components"""
        warnings = []
        hub_names = {hub.name for hub in analysis.hubs}
        link_names = {link.name for link in analysis.links}
        
        # Validate satellites
        for sat in analysis.satellites:
            if sat.hub not in hub_names:
                warnings.append(f"Satellite {sat.name} references non-existent hub: {sat.hub}")
                
        # Validate link satellites
        for lsat in analysis.link_satellites:
            if lsat.link not in link_names:
                warnings.append(f"Link Satellite {lsat.name} references non-existent link: {lsat.link}")
        
        return warnings
    
    def validate_final_result(self, result: DataVaultAnalysis):
        """
        Thực hiện validation cuối cùng cho toàn bộ kết quả
        """
        # Validate unique names
        all_names = (
            [hub.name for hub in result.hubs] +
            [link.name for link in result.links] +
            [sat.name for sat in result.satellites] +
            [lsat.name for lsat in result.link_satellites]
        )
        
        if len(all_names) != len(set(all_names)):
            raise ValueError("Duplicate component names found")
            
        # Validate relationships
        self.validate_relationships(result)

# Khởi tạo và sử dụng
def analyze_metadata(metadata: str, llm) -> Tuple[DataVaultAnalysis, List[str]]:
    """Helper function để phân tích metadata"""
    analyzer = DataVaultAnalyzer(llm)
    
    # Thực hiện phân tích
    analysis = analyzer.analyze(metadata)
    
    # Validate relationships
    warnings = analyzer.validate_relationships(analysis)
    
    return analysis, warnings

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
        
        # In kết quả
        print("\nAnalysis Results:")
        print(f"- Hubs: {len(analysis.hubs)}")
        print(f"- Links: {len(analysis.links)}")
        print(f"- Satellites: {len(analysis.satellites)}")
        print(f"- Link Satellites: {len(analysis.link_satellites)}")
        print(analysis.model_dump_json())
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"- {warning}")
        else:
            print("\nNo warnings - all relationships valid!")
            
    except Exception as e:
        print(f"Analysis failed: {str(e)}")