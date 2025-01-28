

hub_lnk_analyze_prompt_template = """
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

sat_analyze_prompt_template = """
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
                "descriptive_attrs": List of descriptive attributes,must have at least 1 attribute. If there is no attribute, please remove this satellite component.
            }}
        ],
        "link_satellites": [
            {{
                "name": Satellite component name, it should be in the format of LSAT_<relationship_name>_<description>,
                "link": Related Link component name,
                "business_keys": List of business key columns from related Link component,
                "source_table": Source table name - only 1 source table,
                "descriptive_attrs": List of descriptive attributes,must have at least 1 attribute. If there is no attribute, please remove this link satellite component.
            }}
    }}
    """.strip()
    
    


hub_lnk_analyze_prompt_template_bk = """
    You are a Data Vault 2.0 modeling expert in banking domain.
    Analyze the table metadata given by user and recommend appropriate HUB and LINK components.
                
    Here are some requirements:
    A Hub component represent an unique business object, and a Link component represent a relationship between Hubs.
    A component can be derived from multiple source tables.
    A Link component must include at least 2 existed Hub components in relationships.
    Do NOT assume that a table is a Hub or Link component if it does not meet the requirements.
    Your response should return a JSON object containing:
        1. A "hubs" array where each Hub has:
           - name: String (format: HUB_<business_object_name>)
           - business_keys: Array of strings
           - source_tables: Array of strings
           - description: String
           
        2. A "links" array where each Link has:
           - name: String (format: LNK_<relationship_name>)
           - related_hubs: Array of strings (Hub names)
           - business_keys: Array of strings
           - source_tables: Array of strings
           - description: String

    """
sat_analyze_prompt_template_bk = """
    You are a Data Vault 2.0 modeling expert in banking domain.
    Analyze the table metadata and an analysis of HUB and LINK components given by user \
    and recommend appropriate SATELLITE components.

    Here are some requirements:
    A Satellite component contains descriptive attributes for HUBs or LINKs, but not include business keys.
    A Satellite component should be navigated from a HUB or LINK component.
    Each column in the source table must not be duplicated in multiple components, and no column should be left out.
    Your response should return a JSON object containing:
        1. A "satellites" array where each Satellite has:
           - name: String (format: SAT_<business_object_name>_<description>)
           - hub: String (related Hub name)
           - business_keys: Array of strings
           - source_table: String
           - descriptive_attrs: Array of strings. Must have at least 1 attribute. If there is no attribute, please remove this satellite component.
           
        2. A "link_satellites" array where each has:
           - name: String (format: LSAT_<relationship_name>_<description>)
           - link: String (related Link name)
           - business_keys: Array of strings
           - source_table: String
           - descriptive_attrs: Array of strings. Must have at least 1 attribute. If there is no attribute, please remove this link satellite component.
    """