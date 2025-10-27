import os
import io
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from app.models.file import UploadedFile
from app.models.operation import AccountingOperation
from app.services.template_detector import TemplateDetector, TemplateType
from app.services.parsers.rival_parser import RivalParser
from app.services.parsers.ajur_parser import AjurParser
from app.services.parsers.microinvest_parser import MicroinvestParser
from app.services.parsers.incognita_parser import IncognitaParser
from app.services.s3 import S3Service
# Import other parsers as they are implemented
# from app.services.parsers.business_navigator_parser import BusinessNavigatorParser
# from app.services.parsers.universum_parser import UniversumParser


class FileProcessor:
    """Service for processing uploaded Excel files"""
    
    def __init__(self, db: Session):
        self.db = db
        self.template_detector = TemplateDetector()
        
        # Initialize parsers
        self.parsers = {
            TemplateType.RIVAL: RivalParser(),
            TemplateType.AJUR: AjurParser(),
            TemplateType.MICROINVEST: MicroinvestParser(),
            TemplateType.INCOGNITA: IncognitaParser(),
            # Add other parsers as they are implemented
            # TemplateType.BUSINESS_NAVIGATOR: BusinessNavigatorParser(),
            # TemplateType.UNIVERSUM: UniversumParser(),
        }
        
    def _filter_internal_fields(self, operation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out internal fields (starting with underscore) and fields not in the model
        
        Args:
            operation_data: Dictionary containing operation data
            
        Returns:
            Filtered dictionary without internal fields or unsupported fields
        """
        # Fields not in the database model
        unsupported_fields = [
            'analytical_debit_structured',
            'analytical_credit_structured'
        ]
        return {k: v for k, v in operation_data.items()
                if not k.startswith('_') and k not in unsupported_fields}
    
    def create_file(self, filename: str, template_type: str, file_path: str, import_uuid: str) -> UploadedFile:
        """
        Create a record for an uploaded file
        
        Args:
            filename: Original filename
            template_type: Detected template type (e.g., RIVAL, AJUR, etc.)
            file_path: Path where the file is stored
            import_uuid: UUID for grouping files in an import
            
        Returns:
            Created UploadedFile record
        """
        db_file = UploadedFile(
            filename=filename,
            template_type=template_type,
            file_path=file_path,
            processed=False,
            import_uuid=import_uuid
        )
        
        self.db.add(db_file)
        self.db.commit()
        self.db.refresh(db_file)
        return db_file
    
    def process_file(self, file_id: int) -> Optional[List[Dict[str, Any]]]:
        """
        Process a file that has been uploaded
        
        Args:
            file_id: ID of the file to process
            
        Returns:
            List of processed operations or None if processing failed
        """
        # Get file record
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if not file_record:
            print(f"File with ID {file_id} not found")
            return None
        
        try:
            # Download file from S3
            s3_service = S3Service()
            file_content = s3_service.download_file(file_record.file_path)
            
            if not file_content:
                print(f"Could not download file {file_record.file_path} from S3")
                return None
            
            # Create a file-like object from the content
            file_obj = io.BytesIO(file_content)
            
            # Get parser for the template type
            parser = self.parsers.get(file_record.template_type)
            if not parser:
                print(f"No parser available for template type {file_record.template_type}")
                return None
            
            # Parse the file, ensuring import_uuid is passed to parser
            operations = parser.parse_memory(file_obj, file_id, file_record.import_uuid)
            
            print(f"[INFO] Parsed {len(operations)} operations from file {file_id} with import_uuid {file_record.import_uuid}")
            
            # Process operations in batches to avoid large transaction failures
            BATCH_SIZE = 500  # Process 500 operations at a time
            total_operations = len(operations)
            successful_operations = 0
            failed_operations = 0
            
            for batch_start in range(0, total_operations, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_operations)
                batch = operations[batch_start:batch_end]
                print(f"[INFO] Processing batch {batch_start//BATCH_SIZE + 1} ({batch_start} to {batch_end-1}) of {total_operations} operations")
                
                # Process each batch in a separate transaction
                saved_operations_batch = []
                
                for operation_data in batch:
                    # Ensure import_uuid is set correctly in each operation
                    if 'import_uuid' not in operation_data or operation_data['import_uuid'] is None:
                        operation_data['import_uuid'] = file_record.import_uuid
                    
                    try:
                        # Filter out internal fields and create the operation object
                        filtered_data = self._filter_internal_fields(operation_data)
                        operation = AccountingOperation(**filtered_data)
                        self.db.add(operation)
                        saved_operations_batch.append(operation)
                    except Exception as op_error:
                        print(f"[ERROR] Failed to create operation: {op_error}")
                        failed_operations += 1
                        import traceback
                        traceback.print_exc()
                        continue
                
                try:
                    # Commit each batch separately
                    self.db.commit()
                    successful_operations += len(saved_operations_batch)
                    print(f"[INFO] Successfully committed batch with {len(saved_operations_batch)} operations")
                except Exception as batch_error:
                    print(f"[ERROR] Failed to commit batch: {batch_error}")
                    import traceback
                    traceback.print_exc()
                    self.db.rollback()
                    failed_operations += len(batch)
                    
            try:
                # Mark file as processed after all batches have been processed
                file_record.processed = True
                self.db.commit()
                
                print(f"[INFO] Processing completed: {successful_operations} operations saved successfully, {failed_operations} operations failed")
                
                # Verify some operations were saved
                db_operations = self.db.query(AccountingOperation).filter(
                    AccountingOperation.file_id == file_id
                ).count()
                
                print(f"[INFO] Verification: {db_operations} operations found in database for file {file_id}")
                
                # If no operations were saved at all, it's a complete failure
                if db_operations == 0:
                    print(f"[ERROR] Failed to save any operations to database for file {file_id}")
                    return None
                
                if successful_operations < total_operations:
                    print(f"[WARNING] Only {successful_operations} of {total_operations} operations were saved to database from file {file_id}")
            except Exception as commit_error:
                print(f"[ERROR] Failed to commit operations to database: {commit_error}")
                import traceback
                traceback.print_exc()
                self.db.rollback()
                raise
            
            return operations
            
        except Exception as e:
            self.db.rollback()
            print(f"Error processing file {file_id}: {e}")
            return None
    
    def detect_template(self, file_path: str) -> Optional[str]:
        """
        Detect the template type of a file
        
        Args:
            file_path: Path to the file
            
        Returns:
            Template type as string or None if detection failed
        """
        template_type = self.template_detector.detect_template(file_path)
        return template_type.value if template_type else None