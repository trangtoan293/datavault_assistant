
from pydantic import BaseModel,Field,field_validator,model_validator
from typing import Dict, List, Optional, Any

    
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


from dataclasses import dataclass

@dataclass
class DataVaultValidationError(Exception):
    message: str
    

class HubComponent(BaseModel):
    """
    Represents a Hub component in Data Vault 2.0 modeling.
    A Hub uniquely identifies a business object and can be sourced from multiple tables.
    """
    name: str = Field(
        ...,  # ... means required field
        description="Hub component name in format HUB_<business_object_name>",
        pattern="^HUB_[A-Z0-9_]+$"  # Enforce naming convention
    )
    business_keys: List[str] = Field(
        ...,
        description="List of business key columns that uniquely identify the business object",
        min_items=1  # Must have at least one business key
    )
    source_tables: List[str] = Field(
        ...,
        description="List of source tables contributing to this Hub",
        min_items=1  # Must have at least one source table
    )
    description: str = Field(
        ...,
        description="Short description explaining the business purpose of this Hub"
    )


class LinkComponent(BaseModel):
    """
    Represents a Link component in Data Vault 2.0 modeling.
    A Link captures relationships between two or more Hubs.
    """
    name: str = Field(
        ...,
        description="Link component name in format LNK_<relationship_name>",
        pattern="^LNK_[A-Z0-9_]+$"  # Enforce naming convention
    )
    related_hubs: List[str] = Field(
        ...,
        description="List of Hub components involved in this relationship",
        min_items=2 
    )
    business_keys: List[str] = Field(
        ...,
        description="List of business key columns, including all keys from related Hubs",
        min_items=1
    )
    source_tables: List[str] = Field(
        ...,
        description="List of source tables contributing to this Link, source tables must contain all business keys from related Hubs",
        min_items=1
    )
    description: str = Field(
        ...,
        description="Short description explaining the relationship captured by this Link"
    )
    
    @field_validator('related_hubs')
    def validate_related_hubs(cls, v):
        if not all(hub.startswith('HUB_') for hub in v):
            raise ValueError("All related hubs must start with 'HUB_'")
        return v


class SatelliteComponent(BaseModel):
    """Model cho Satellite component trong Data Vault 2.0"""
    name: str = Field(
        ...,
        description="Satellite name in format SAT_<business_object_name>_<description>",
        pattern="^SAT_[A-Z0-9_]+$"
    )
    hub: str = Field(
        ...,
        description="Related Hub component name"
    )
    business_keys: List[str] = Field(
        ...,
        description="Business keys from related Hub",
        min_items=1
    )
    source_table: str = Field(
        ...,
        description="Source table name"
    )
    descriptive_attrs: List[str] = Field(
        ...,
        description="List of descriptive attributes",
        # min_items=1
    )

class LinkSatelliteComponent(BaseModel):
    """Model cho Link Satellite component trong Data Vault 2.0"""
    name: str = Field(
        ...,
        description="Link Satellite name in format LSAT_<relationship_name>_<description>",
        pattern="^LSAT_[A-Z0-9_]+$"
    )
    link: str = Field(
        ...,
        description="Related Link component name"
    )
    business_keys: List[str] = Field(
        ...,
        description="Business keys from related Link",
        min_items=1
    )
    source_table: str = Field(
        ...,
        description="Source table name"
    )
    descriptive_attrs: List[str] = Field(
        ...,
        description="List of descriptive attributes",
        # min_items=1
    )

class HubLinkAnalysis(BaseModel):
    """Kết quả phân tích Hub và Link"""
    hubs: List[HubComponent] = Field(..., min_items=1)
    links: List[LinkComponent] = Field(default_factory=list)

class SatelliteAnalysis(BaseModel):
    """Kết quả phân tích Satellite và Link Satellite"""
    satellites: List[SatelliteComponent] = Field(default_factory=list)
    link_satellites: List[LinkSatelliteComponent] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    hub_analysis: Optional[dict] = Field(None, exclude=True)  # Thêm trường này để lưu hub analysis

    @model_validator(mode='before')
    def validate_full_analysis(cls, values):
        """
        Validate toàn bộ analysis, đặc biệt là mối quan hệ giữa Hub và Satellite.
        Phương thức này sẽ kiểm tra sự khớp nhau của business keys.
        """
        if not isinstance(values, dict):
            return values
        
        warnings = []
        hub_analysis = values.get('hub_analysis', {})
        
        # Tạo các mapping cần thiết cho validation
        hub_keys_map = {
            hub['name']: set(hub['business_keys']) 
            for hub in hub_analysis.get('hubs', [])
        }
        
        link_keys_map = {
            link['name']: set(link['business_keys'])
            for link in hub_analysis.get('links', [])
        }


        # Validate satellites
        if 'satellites' in values:
            valid_sats = []
            for idx, sat in enumerate(values['satellites']):
                try:
                    if isinstance(sat, dict):
                        # Validate business keys với hub tương ứng
                        hub_name = sat.get('hub')
                        if hub_name in hub_keys_map:
                            hub_keys = hub_keys_map[hub_name]
                            sat_keys = set(sat.get('business_keys', []))
                            
                            # So sánh business keys
                            if sat_keys != hub_keys:
                                missing_keys = hub_keys - sat_keys
                                extra_keys = sat_keys - hub_keys
                                
                                if missing_keys:
                                    warnings.append(
                                        f"Satellite {sat.get('name')}: Missing business keys from hub {hub_name}: "
                                        f"{', '.join(missing_keys)}"
                                    )
                                    # Tự động thêm missing keys
                                    sat['business_keys'] = list(hub_keys)
                                
                                if extra_keys:
                                    warnings.append(
                                        f"Satellite {sat.get('name')}: Has extra business keys not in hub {hub_name}: "
                                        f"{', '.join(extra_keys)}"
                                    )
                                    # Loại bỏ extra keys
                                    sat['business_keys'] = list(hub_keys)
                        else:
                            warnings.append(
                                f"Satellite {sat.get('name')}: References non-existent hub {hub_name}"
                            )

                        # Validate descriptive attributes
                        if not sat.get('descriptive_attrs'):
                            warnings.append(f"Satellite {sat.get('name')}: Missing descriptive attributes")
                            sat['descriptive_attrs'] = ["PLACEHOLDER_ATTR"]
                            
                    valid_sats.append(sat)
                except Exception as e:
                    warnings.append(f"Invalid Satellite at index {idx}: {str(e)}")
                    
            values['satellites'] = valid_sats
        # Validate link satellites
        if 'link_satellites' in values:
            valid_lsats = []
            for idx, lsat in enumerate(values['link_satellites']):
                try:
                    if isinstance(lsat, dict):
                        # Validate business keys với link tương ứng
                        link_name = lsat.get('link')
                        if link_name in link_keys_map:
                            link_keys = link_keys_map[link_name]
                            lsat_keys = set(lsat.get('business_keys', []))
                            
                            # So sánh business keys
                            if lsat_keys != link_keys:
                                missing_keys = link_keys - lsat_keys
                                extra_keys = lsat_keys - link_keys
                                
                                if missing_keys:
                                    warnings.append(
                                        f"Link Satellite {lsat.get('name')}: Missing business keys from link {link_name}: "
                                        f"{', '.join(missing_keys)}"
                                    )
                                    # Tự động thêm missing keys
                                    lsat['business_keys'] = list(link_keys)
                                
                                if extra_keys:
                                    warnings.append(
                                        f"Link Satellite {lsat.get('name')}: Has extra business keys not in link {link_name}: "
                                        f"{', '.join(extra_keys)}"
                                    )
                                    # Loại bỏ extra keys để đảm bảo tính nhất quán
                                    lsat['business_keys'] = list(link_keys)
                        else:
                            warnings.append(
                                f"Link Satellite {lsat.get('name')}: References non-existent link {link_name}"
                            )

                        # Validate source table
                        if not lsat.get('source_table'):
                            warnings.append(
                                f"Link Satellite {lsat.get('name')}: Missing source table"
                            )

                        # Validate descriptive attributes
                        if not lsat.get('descriptive_attrs'):
                            warnings.append(
                                f"Link Satellite {lsat.get('name')}: Missing descriptive attributes"
                            )
                            lsat['descriptive_attrs'] = ["PLACEHOLDER_ATTR"]

                        # Validate naming convention
                        if not lsat.get('name', '').startswith('LSAT_'):
                            warnings.append(
                                f"Link Satellite {lsat.get('name')}: Name should start with 'LSAT_'"
                            )
                            
                    valid_lsats.append(lsat)
                except Exception as e:
                    warnings.append(f"Invalid Link Satellite at index {idx}: {str(e)}")
                    
            values['link_satellites'] = valid_lsats
            
        # Tối ưu hóa warnings bằng cách gom nhóm các cảnh báo tương tự
        grouped_warnings = {}
        for warning in warnings:
            warning_type = warning.split(':')[0]
            if warning_type not in grouped_warnings:
                grouped_warnings[warning_type] = []
            grouped_warnings[warning_type].append(warning)

        # Sắp xếp và format warnings
        formatted_warnings = []
        for warning_type, warning_list in grouped_warnings.items():
            if len(warning_list) > 1:
                formatted_warnings.append(f"{warning_type}: Found {len(warning_list)} issues:")
                formatted_warnings.extend(f"  - {w.split(':', 1)[1]}" for w in warning_list)
            else:
                formatted_warnings.extend(warning_list)

        values['warnings'] = formatted_warnings
        return values

class DataVaultAnalysis(BaseModel):
    """Kết quả phân tích toàn bộ Data Vault"""
    hubs: List[HubComponent] = Field(..., min_items=1)
    links: List[LinkComponent] = Field(default_factory=list)
    satellites: List[SatelliteComponent] = Field(default_factory=list)
    link_satellites: List[LinkSatelliteComponent] = Field(default_factory=list)