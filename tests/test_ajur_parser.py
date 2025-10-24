import os
import pytest
import pandas as pd
from datetime import datetime
from io import BytesIO

from app.services.parsers.ajur_parser import AjurParser
from app.services.template_detector import TemplateDetector


@pytest.fixture
def sample_ajur_file_path():
    """Fixture to provide the path to a sample AJUR file"""
    # Use the real Ajur file in the 'files' directory
    return os.path.join("files", "ajur.xlsx")


def test_ajur_template_detection(sample_ajur_file_path):
    """Test that the template detector correctly identifies Ajur files"""
    detector = TemplateDetector()
    template_type = detector.detect_template(sample_ajur_file_path)
    
    assert template_type == "ajur", f"Expected 'ajur' template type but got '{template_type}'"
    print(f"Successfully detected template type: {template_type}")


def test_ajur_parse_file(sample_ajur_file_path):
    """Test parsing a sample Ajur file"""
    parser = AjurParser()
    operations = parser.parse(sample_ajur_file_path, file_id=1)
    
    # We should have multiple operations from the sample file
    assert len(operations) > 0, "No operations were extracted from the Ajur file"
    print(f"Successfully extracted {len(operations)} operations from Ajur file")
    
    # Check the structure of the first operation
    first_op = operations[0]
    required_fields = [
        "file_id", "operation_date", "document_type", "document_number", 
        "debit_account", "credit_account", "amount", "description", 
        "template_type"
    ]
    
    for field in required_fields:
        assert field in first_op, f"Required field '{field}' missing from operation"
    
    # Check specific data values
    assert first_op["template_type"] == "ajur", "Template type should be 'ajur'"
    assert first_op["file_id"] == 1, "File ID should be 1"
    
    # Check accounts and amount (using basic validation rules)
    assert first_op["debit_account"] or first_op["credit_account"], "At least one account must be present"
    assert first_op["amount"] > 0, "Amount should be positive"
    
    # Print the first operation for manual inspection
    print("\nFirst operation details:")
    for key, value in first_op.items():
        if key != "raw_data":  # Skip the raw data as it's too verbose
            print(f"  {key}: {value}")


def test_ajur_parse_memory(sample_ajur_file_path):
    """Test parsing an Ajur file from memory"""
    parser = AjurParser()
    
    # Read file into memory
    with open(sample_ajur_file_path, "rb") as f:
        file_content = f.read()
    
    # Parse from memory
    file_obj = BytesIO(file_content)
    operations = parser.parse_memory(file_obj, file_id=1)
    
    # We should have multiple operations
    assert len(operations) > 0, "No operations were extracted from in-memory Ajur file"
    print(f"Successfully extracted {len(operations)} operations from in-memory Ajur file")
    
    # Check template type for all operations
    for op in operations:
        assert op["template_type"] == "ajur", "Template type should be 'ajur'"
    
    # Verify accounts format - at least some operations should have accounts with slashes (/)
    accounts_with_slash = [op for op in operations if 
                          (op["debit_account"] and "/" in op["debit_account"]) or 
                          (op["credit_account"] and "/" in op["credit_account"])]
    
    assert len(accounts_with_slash) > 0, "No operations found with properly formatted account numbers"
    print(f"Found {len(accounts_with_slash)} operations with properly formatted account numbers")


def test_ajur_data_extraction(sample_ajur_file_path):
    """Test detailed data extraction from Ajur file"""
    parser = AjurParser()
    operations = parser.parse(sample_ajur_file_path, file_id=1)
    
    # We should have extracted most operations from the file
    # The sample file has at least 50 rows, so expect at least 40 operations (allowing for some skipped rows)
    assert len(operations) >= 40, f"Expected at least 40 operations but got {len(operations)}"
    
    # Check for operations with both analytical accounts
    ops_with_analytical = [op for op in operations if op.get("analytical_debit") and op.get("analytical_credit")]
    assert len(ops_with_analytical) > 0, "No operations found with both analytical accounts"
    
    # Test date conversion
    for op in operations:
        assert isinstance(op["operation_date"], (datetime, str)), "Operation date should be a datetime or string"
    
    # Check document types
    doc_types = {op["document_type"] for op in operations if op.get("document_type")}
    print(f"Found document types: {doc_types}")
    assert len(doc_types) > 0, "No document types extracted"


def test_ajur_partner_extraction(sample_ajur_file_path):
    """Test partner name extraction and description enhancement from analytical accounts"""
    parser = AjurParser()
    operations = parser.parse(sample_ajur_file_path, file_id=1)
    
    # Verify some operations have partner names extracted
    ops_with_partners = [op for op in operations if op.get("partner_name")]
    assert len(ops_with_partners) > 0, "No operations found with extracted partner names"
    print(f"Found {len(ops_with_partners)} operations with partner names")
    
    # Print some examples of extracted partner names
    partner_samples = ops_with_partners[:3] if len(ops_with_partners) >= 3 else ops_with_partners
    print("\nPartner name extraction examples:")
    for i, op in enumerate(partner_samples):
        print(f"  Example {i+1}:")
        print(f"    Partner: {op['partner_name']}")
        print(f"    Analytical debit: {op.get('analytical_debit', 'None')}")
        print(f"    Analytical credit: {op.get('analytical_credit', 'None')}")
    
    # Check for enhanced descriptions
    # Count operations where description matches analytical content
    meaningful_descriptions = [
        op for op in operations
        if op.get("description") and (
            op.get("analytical_debit_structured", {}).get("description", "") in op["description"] or
            op.get("analytical_credit_structured", {}).get("description", "") in op["description"]
        )
    ]
    
    print(f"\nFound {len(meaningful_descriptions)} operations with meaningful descriptions")
    
    # Ensure we're properly using structured data
    structured_data_check = [
        op for op in operations
        if (op.get("analytical_debit_structured") or op.get("analytical_credit_structured"))
    ]
    
    assert len(structured_data_check) > 0, "No operations found with structured analytical data"
    print(f"Found {len(structured_data_check)} operations with structured analytical data")


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])