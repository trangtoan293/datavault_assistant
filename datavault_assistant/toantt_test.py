
yml_context = """
source_schema: COREBANK
source_table: COREBANK_CUSTOMER
target_schema: INTEGRATION
target_table: LNK_CUSTOMER_CARD
target_entity_type: lnk
collision_code: MDM
description: A customer can have multiple cards.
metadata:
  created_at: '2025-01-08T17:57:43.094894'
  version: 1.0.0
  validation_status: warnings
  validation_warnings:
  - 'Link LNK_CUSTOMER_CARD is missing business keys from hub HUB_CUSTOMER: [''CB_CIF_NO'']'
columns:
- target: DV_HKEY_LNK_CUSTOMER_CARD
  dtype: raw
  key_type: hash_key_lnk
  source:
  - CUS_CUSTOMER_CODE
  - CB_CUSTOMER_IDNO
- target: DV_HKEY_HUB_CUSTOMER
  dtype: raw
  key_type: hash_key_hub
  parent: HUB_CUSTOMER
  source:
  - name: CUS_CUSTOMER_CODE
    dtype: number
- target: DV_HKEY_HUB_CARD
  dtype: raw
  key_type: hash_key_hub
  parent: HUB_CARD
  source:
  - name: CB_CUSTOMER_IDNO
    dtype: VARCHAR2(255)



"""
import yaml
yaml_data = yaml.safe_load(yml_context)
for idx, column in enumerate(yaml_data['columns']):
    print('idx:', idx)
    print('column:', column)
    source_info = column.get('source')
    source_columns = []
    transformation_rule = None
    target_dtype = column.get('dtype')
    
    # Process source column information
    if source_info:
        if isinstance(source_info, dict):
            # Single source column with data type
            source_columns = [{
                'name': source_info['name'],
                'dtype': source_info.get('dtype')
            }]
        elif isinstance(source_info, list):
            # Multiple source columns
            if isinstance(source_info[0], dict):
                source_columns = source_info
            else:
                # Get data types from source system if available
                source_columns = []
                for src in source_info:
                    if isinstance(src, dict):
                        source_columns.append(src)
                    else:
                        # For string inputs, get data type from source system
                        # source_dtype = self._get_source_column_dtype(source_table_id, src)
                        source_columns.append({
                            'name': src,
                            'dtype': 1
                        })
    print('source_columns:', source_columns)