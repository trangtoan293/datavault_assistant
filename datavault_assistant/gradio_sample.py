import gradio as gr
from datavault_assistant.core.nodes.data_vault_parser import DataProcessor
from datavault_assistant.core.nodes.data_vault_builder import DataVaultAnalyzer
from datavault_assistant.core.metadata.source_handler import SourceMetadataProcessor
from datavault_assistant.core.utils.db_handler import DatabaseHandler
from datavault_assistant.configs.settings import ParserConfig, get_settings
from datavault_assistant.core.utils.llm import init_llm
from pathlib import Path
import pandas as pd
import json

class DataVaultUI:
    def __init__(self):
        # Khởi tạo các components
        settings = get_settings()
        self.db_config = {
            'dbname': settings.DB_NAME,
            'user': settings.DB_USER,
            'password': settings.DB_PASSWORD,
            'host': settings.DB_HOST,
            'port': settings.DB_PORT
        }
        self.config = ParserConfig()
        self.processor = DataProcessor(self.config)
        self.analyzer = DataVaultAnalyzer(init_llm(provider="groq"))
        
    def process_metadata(self, file, system_name, user_id):
        """Hàm xử lý metadata từ file Excel"""
        try:
            # Đọc file metadata
            metadata = pd.read_excel(file.name)
            
            # Xử lý source metadata
            db = DatabaseHandler(self.db_config)
            source_processor = SourceMetadataProcessor(
                db_handler=db,
                system_name=system_name,
                user_id=user_id
            )
            source_processor.process_source_metadata(metadata)
            
            # Analyze với LLM
            result = self.analyzer.analyze(metadata.to_string(index=False))
            
            # Process data và generate output
            output_path = Path("output")
            self.processor.process_data(
                input_data=result,
                mapping_data=metadata,
                output_dir=output_path
            )
            
            # Đọc kết quả từ files để hiển thị
            output_files = list(output_path.glob("*.yaml"))
            output_content = []
            for file in output_files:
                with open(file, 'r') as f:
                    output_content.append(f"{file.name}:\n{f.read()}\n")
            
            return "\n".join(output_content), "Processing completed successfully!"
            
        except Exception as e:
            return None, f"Error occurred: {str(e)}"

def create_ui():
    # Khởi tạo UI handler
    ui_handler = DataVaultUI()
    
    # Tạo Gradio interface
    with gr.Blocks(theme=gr.themes.Soft()) as demo:
        gr.Markdown("# Data Vault Assistant")
        
        with gr.Row():
            with gr.Column():
                # Input components
                file_input = gr.File(
                    label="Upload Metadata Excel File",
                    file_types=[".xlsx"]
                )
                system_name = gr.Textbox(
                    label="System Name",
                    placeholder="Enter system name...",
                    value="FLEXLIVE"
                )
                user_id = gr.Textbox(
                    label="User ID",
                    placeholder="Enter user ID...",
                    value="admin"
                )
                process_btn = gr.Button("Process Metadata", variant="primary")
            
            with gr.Column():
                # Output components
                output_text = gr.TextArea(
                    label="Generated YAML Files",
                    interactive=False,
                    lines=20
                )
                status = gr.Textbox(
                    label="Status",
                    interactive=False
                )
        
        # Event handling
        process_btn.click(
            fn=ui_handler.process_metadata,
            inputs=[file_input, system_name, user_id],
            outputs=[output_text, status]
        )
        
    return demo

if __name__ == "__main__":
    demo = create_ui()
    demo.launch(share=False)