from pathlib import Path
from typing import Dict, Any, List, Optional,Union
from datetime import datetime
import pandas as pd 
import logging
import yaml
import json
from configs.settings import ParserConfig
from core.nodes.hub_parser import HubParser
from core.nodes.link_parser import LinkParser
from core.nodes.sat_parser import SatelliteParser
from core.nodes.lsat_parser import LinkSatelliteParser

class FileProcessor:
    """Class xử lý file I/O operations"""
    
    def __init__(self, 
                 output_dir: Optional[Path] = None, 
                 encoding: str = 'utf-8'):
        """
        Initialize FileProcessor
        
        Args:
            output_dir: Optional directory path for output files
            encoding: File encoding (default: utf-8)
        """
        self.output_dir = output_dir
        self.encoding = encoding
        self.logger = logging.getLogger(self.__class__.__name__)

    def _read_json(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Read and parse JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Dict containing parsed JSON data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If JSON parsing fails
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File không tồn tại: {file_path}")
                
            with open(file_path, 'r', encoding=self.encoding) as f:
                return json.load(f)
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Lỗi parse JSON từ {file_path}: {str(e)}")
            raise ValueError(f"Lỗi parse JSON: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"Unexpected error reading {file_path}: {str(e)}")
            raise

    def _read_csv(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """
        Read CSV file into DataFrame
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            pandas DataFrame
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File không tồn tại: {file_path}")
                
            return pd.read_csv(file_path, **kwargs)
            
        except Exception as e:
            self.logger.error(f"Error reading CSV {file_path}: {str(e)}")
            raise

    def _save_yaml(self, 
                 data: Dict[str, Any], 
                 output_path: Union[str, Path],
                 allow_unicode: bool = True,
                 sort_keys: bool = False) -> None:
        """
        Save data to YAML file
        
        Args:
            data: Data to save
            output_path: Output file path
            allow_unicode: Allow unicode in output (default: True)  
            sort_keys: Sort dictionary keys (default: False)
        """
        
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding=self.encoding) as f:
                yaml.dump(data, 
                         f,
                         allow_unicode=allow_unicode,
                         sort_keys=sort_keys)
                
            self.logger.info(f"Successfully saved YAML to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving YAML to {output_path}: {str(e)}")
            raise

    def _save_processing_summary(self,
                              results: List[Dict[str, Any]],
                              output_dir: Path,
                              entity_type: str = "entity") -> None:
        """
        Save processing summary to YAML file
        
        Args:
            results: List of processing results
            output_dir: Output directory
            entity_type: Type of entity being processed
        """
        try:
            summary = {
                "processing_summary": {
                    "processed_at": datetime.now().isoformat(),
                    f"total_{entity_type}s": len(results),
                    "successful": sum(1 for r in results if r["status"] != "error"),
                    "warnings": sum(1 for r in results if r["status"] == "warnings"),
                    "errors": sum(1 for r in results if r["status"] == "error"),
                    "details": results
                }
            }
            
            summary_file = Path(output_dir) / f"processing_{entity_type}_summary.yaml"
            self.save_yaml(summary, summary_file)
            
        except Exception as e:
            self.logger.error(f"Error saving processing summary: {str(e)}")
            raise

    def _ensure_output_directory(self, directory: Optional[Union[str, Path]] = None) -> Path:
        """
        Ensure output directory exists
        
        Args:
            directory: Optional directory path, uses self.output_dir if not provided
            
        Returns:
            Path object for output directory
        """
        try:
            output_dir = Path(directory or self.output_dir)
            if not output_dir:
                raise ValueError("No output directory specified")
                
            output_dir.mkdir(parents=True, exist_ok=True)
            return output_dir
            
        except Exception as e:
            self.logger.error(f"Error ensuring output directory: {str(e)}")
            raise

    def get_output_path(self, 
                       filename: str, 
                       directory: Optional[Union[str, Path]] = None,
                       extension: Optional[str] = None) -> Path:
        """
        Get full output path for a file
        
        Args:
            filename: Base filename
            directory: Optional directory path
            extension: Optional file extension to add/replace
            
        Returns:
            Full output Path
        """
        try:
            output_dir = self._ensure_output_directory(directory)
            
            # Add/replace extension if provided
            if extension:
                if not extension.startswith('.'):
                    extension = f".{extension}"
                filename = f"{Path(filename).stem}{extension}"
                
            return output_dir / filename
            
        except Exception as e:
            self.logger.error(f"Error getting output path: {str(e)}")
            raise


    
class DataProcessor:
    """Class xử lý data cho tất cả loại entities trong Data Vault"""
    
    def __init__(self, config: ParserConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize all parsers
        self.hub_parser = HubParser(config)
        self.link_parser = LinkParser(config)
        self.sat_parser = SatelliteParser(config)
        self.lsat_parser = LinkSatelliteParser(config)
        
        # Create file processor for saving files
        self.file_processor = FileProcessor()  # Parser sẽ được set sau
        
    def process_data(self, 
                    input_data: Dict[str, Any], 
                    mapping_data: pd.DataFrame, 
                    output_dir: Path) -> Dict[str, Any]:
        """
        Process data trực tiếp từ input
        
        Args:
            input_data: Dict chứa data input
            mapping_data: DataFrame chứa mapping data  
            output_dir: Path to output directory
            
        Returns:
            Dict chứa kết quả xử lý của tất cả entities
        """
        try:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            results = {
                "hubs": [],
                "links": [],
                "satellites": [],
                "link_satellites": []
            }
            
            # Process hubs
            if "hubs" in input_data:
                results["hubs"] = self._process_hubs(input_data, mapping_data, output_dir)
            
            # Process links (sau khi có hub metadata)
            if "links" in input_data:
                results["links"] = self._process_links(input_data, mapping_data, output_dir)
            
            # Process satellites
            if "satellites" in input_data:
                results["satellites"] = self._process_satellites(input_data, mapping_data, output_dir)
                
            # Process link satellites (sau khi có link metadata)
            if "link_satellites" in input_data:
                results["link_satellites"] = self._process_link_satellites(input_data, mapping_data, output_dir)
            
            # Save summary for each entity type
            self._save_summaries(results, output_dir)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise
    
    def process_file(self, input_file: Path, mapping_file: Path, output_dir: Path) -> Dict[str, Any]:
        """Process từ input files"""
        try:
            input_data = self.file_processor._read_json(input_file)
            mapping_data = pd.read_csv(mapping_file)
            return self.process_data(input_data, mapping_data, output_dir)
        except Exception as e:
            self.logger.error(f"Error processing from file: {str(e)}")
            raise
            
    def _process_hubs(self, input_data: Dict[str, Any], mapping_data: pd.DataFrame, 
                     output_dir: Path) -> List[Dict[str, Any]]:
        """Process hub entities"""
        results = []
        
        for hub in input_data.get("hubs", []):
            try:
                self.logger.info(f"Processing hub: {hub['name']}")
                result = self.hub_parser.parse(hub, mapping_data)
                
                # Save output
                output_file = output_dir / f"{hub['name'].lower()}_metadata.yaml"
                self.file_processor._save_yaml(result, output_file)
                
                results.append({
                    "hub": hub["name"],
                    "status": result["metadata"]["validation_status"],
                    "warnings": result["metadata"].get("validation_warnings", [])
                })
                
            except Exception as e:
                self.logger.error(f"Error processing hub {hub.get('name')}: {str(e)}")
                results.append({
                    "hub": hub.get("name"),
                    "status": "error",
                    "error": str(e)
                })
                
        return results
        
    def _process_links(self, input_data: Dict[str, Any], mapping_data: pd.DataFrame, 
                      output_dir: Path) -> List[Dict[str, Any]]:
        """Process link entities"""
        results = []
        
        # Cache hub metadata
        self.link_parser.hub_service.cache_hubs_metadata(input_data)
        
        for link in input_data.get("links", []):
            try:
                self.logger.info(f"Processing link: {link['name']}")
                result = self.link_parser.parse(link, mapping_data)
                
                output_file = output_dir / f"{link['name'].lower()}_metadata.yaml"
                self.file_processor._save_yaml(result, output_file)
                
                results.append({
                    "link": link["name"],
                    "status": result["metadata"]["validation_status"],
                    "warnings": result["metadata"].get("validation_warnings", [])
                })
                
            except Exception as e:
                self.logger.error(f"Error processing link {link.get('name')}: {str(e)}")
                results.append({
                    "link": link.get("name"),
                    "status": "error",
                    "error": str(e)
                })
                
        return results
        
    def _process_satellites(self, input_data: Dict[str, Any], mapping_data: pd.DataFrame,
                          output_dir: Path) -> List[Dict[str, Any]]:
        """Process satellite entities"""
        results = []
        
        for sat in input_data.get("satellites", []):
            try:
                self.logger.info(f"Processing satellite: {sat['name']}")
                result = self.sat_parser.parse(sat, mapping_data)
                
                output_file = output_dir / f"{sat['name'].lower()}_metadata.yaml"
                self.file_processor._save_yaml(result, output_file)
                
                results.append({
                    "satellite": sat["name"],
                    "status": result["metadata"]["validation_status"],
                    "warnings": result["metadata"].get("validation_warnings", [])
                })
                
            except Exception as e:
                self.logger.error(f"Error processing satellite {sat.get('name')}: {str(e)}")
                results.append({
                    "satellite": sat.get("name"),
                    "status": "error",
                    "error": str(e)
                })
                
        return results
        
    def _process_link_satellites(self, input_data: Dict[str, Any], mapping_data: pd.DataFrame,
                               output_dir: Path) -> List[Dict[str, Any]]:
        """Process link satellite entities"""
        results = []
        
        # Cache link metadata
        self.lsat_parser._cache_links_metadata(input_data)
        
        for lsat in input_data.get("link_satellites", []):
            try:
                self.logger.info(f"Processing link satellite: {lsat['name']}")
                result = self.lsat_parser.parse(lsat, mapping_data)
                
                output_file = output_dir / f"{lsat['name'].lower()}_metadata.yaml"
                self.file_processor._save_yaml(result, output_file)
                
                results.append({
                    "link_satellite": lsat["name"],
                    "status": result["metadata"]["validation_status"],
                    "warnings": result["metadata"].get("validation_warnings", [])
                })
                
            except Exception as e:
                self.logger.error(f"Error processing link satellite {lsat.get('name')}: {str(e)}")
                results.append({
                    "link_satellite": lsat.get("name"),
                    "status": "error",
                    "error": str(e)
                })
                
        return results
    
    def _save_summaries(self, results: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> None:
        """Save processing summaries for all entity types"""
        entity_types = {
            "hubs": "processing_hub_summary.yaml",
            "links": "processing_link_summary.yaml", 
            "satellites": "processing_sat_summary.yaml",
            "link_satellites": "processing_lsat_summary.yaml"
        }
        
        for entity_type, filename in entity_types.items():
            if results[entity_type]:
                entity_results = results[entity_type]
                summary = {
                    "processing_summary": {
                        "processed_at": datetime.now().isoformat(),
                        f"total_{entity_type}": len(entity_results),
                        "successful": sum(1 for r in entity_results if r["status"] != "error"),
                        "warnings": sum(1 for r in entity_results if r["status"] == "warnings"),
                        "errors": sum(1 for r in entity_results if r["status"] == "error"),
                        "details": entity_results
                    }
                }
                
                summary_file = output_dir / filename
                self.file_processor._save_yaml(summary, summary_file)

# Example usage
def main():
    config = ParserConfig()
    processor = DataProcessor(config)
    
    try:
        # Example 1: Process from file
        results = processor.process_file(
            input_file=Path("data/sat_lsat.json"),
            mapping_file=Path("data/metadata_src.csv"),
            output_dir=Path("output")
        )
        
        # Example 2: Process from direct data
        input_data = {
            "hubs": [...],
            "links": [...],
            "satellites": [...],
            "link_satellites": [...]
        }
        
        mapping_data = pd.DataFrame({...})
        
        results = processor.process_data(
            input_data=input_data,
            mapping_data=mapping_data,
            output_dir=Path("output")
        )
        
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()