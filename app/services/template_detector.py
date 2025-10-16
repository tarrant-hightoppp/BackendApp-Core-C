from enum import Enum
from typing import Optional, IO, List, Union
import pandas as pd
from io import BytesIO


class TemplateType(str, Enum):
    RIVAL = "RIVAL"
    AJUR = "AJUR"
    MICROINVEST = "MICROINVEST"  # Changed from MIKROINVEST for consistency
    BUSINESS_NAVIGATOR = "BusinessNavigator"
    UNIVERSUM = "UNIVERSUM"


class TemplateDetector:
    """Service for detecting the template type of an Excel file"""

    def detect_template(self, file_path: str) -> Optional[TemplateType]:
        """
        Detect the template type based on file content and structure
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            TemplateType or None if the template cannot be detected
        """
        try:
            # Read first few rows to analyze headers
            df = pd.read_excel(file_path, nrows=20)  # Increased to 20 rows to accommodate header scanning
            return self._analyze_dataframe(df)
        except Exception as e:
            # Log the error
            print(f"Error detecting template: {e}")
            return None
    
    def detect_template_from_bytes(self, file_obj: Union[BytesIO, bytes]) -> Optional[TemplateType]:
        """
        Detect the template type from file content in memory
        
        Args:
            file_obj: BytesIO object or bytes containing the Excel file
            
        Returns:
            TemplateType or None if the template cannot be detected
        """
        try:
            print("[DEBUG] Starting detect_template_from_bytes")
            print("[DEBUG] File object type:", type(file_obj))
            
            # If we received bytes, convert to BytesIO
            if isinstance(file_obj, bytes):
                file_obj = BytesIO(file_obj)
                print("[DEBUG] Converted bytes to BytesIO")
                
            # Reset file pointer to beginning just in case
            file_obj.seek(0)
            
            # Attempt to detect file format
            try:
                print("[DEBUG] Checking file size")
                file_size = file_obj.getbuffer().nbytes if hasattr(file_obj, 'getbuffer') else None
                print(f"[DEBUG] File size: {file_size} bytes")
                
                # Reset again after checking size
                file_obj.seek(0)
            except Exception as e:
                print(f"[WARNING] Could not check file size: {e}")
            
            # Read first few rows to analyze headers
            print("[DEBUG] Reading Excel file with pandas")
            try:
                # Try with default engine first
                df = pd.read_excel(file_obj, nrows=20)
            except Exception as xls_error:
                print(f"[WARNING] Error with default engine: {xls_error}, trying with 'xlrd' engine")
                # Reset file pointer
                file_obj.seek(0)
                # Try with xlrd engine for older .xls files
                df = pd.read_excel(file_obj, nrows=20, engine='xlrd')
                
            print(f"[DEBUG] DataFrame shape: {df.shape}")
            
            # Print headers as a list
            headers = [str(col) for col in df.columns]
            print(f"[DEBUG] Excel headers: {headers}")
            
            # Print first few rows for debugging
            print("[DEBUG] First rows of data:")
            print(df.head(5).to_string())
            
            # Additional debugging: print some sample data values
            print("[DEBUG] Sample data values from first 3 rows:")
            for i in range(min(3, len(df))):
                row_data = {f"Col {j}": str(val) for j, val in enumerate(df.iloc[i].values)}
                print(f"Row {i}: {row_data}")
            
            return self._analyze_dataframe(df)
        except Exception as e:
            # Log the error
            print(f"[ERROR] Error detecting template from bytes: {e}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            return None
    
    def _analyze_dataframe(self, df: pd.DataFrame) -> Optional[TemplateType]:
        """
        Analyze a DataFrame to determine the template type
        
        Args:
            df: Pandas DataFrame containing the Excel data
            
        Returns:
            TemplateType or None if the template cannot be detected
        """
        print("[DEBUG] Starting _analyze_dataframe")
        
        # Convert headers to lowercase strings for easier comparison
        headers = self._get_headers(df)
        print(f"[DEBUG] Lowercase headers: {headers}")
        
        # Quick scan for AJUR-specific indicators first to avoid misclassification
        try:
            ajur_specific_indicators = ["установено при одита", "отклонение", "тествани на контролни действия"]
            for i in range(min(5, len(df))):
                row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
                row_text = ' '.join(row_values)
                for indicator in ajur_specific_indicators:
                    if indicator.lower() in row_text:
                        print(f"[DEBUG] Found strong AJUR indicator '{indicator}' in row {i}")
                        if self._check_ajur_pattern(df, headers):
                            print("[DEBUG] Confirmed as AJUR template")
                            return TemplateType.AJUR
        except Exception as e:
            print(f"[DEBUG] Error in AJUR pre-check: {e}")
        
        # Check for specific filename patterns first
        try:
            # If this is a file path property in the dataframe, check it
            file_path = getattr(df, '_metadata', {}).get('file_path', '')
            print(f"[DEBUG] Checking file path: {file_path}")
            
            # Check filename for clues
            if file_path and isinstance(file_path, str):
                if "ривал" in file_path.lower():
                    print(f"[DEBUG] Found 'ривал' in filename: {file_path}")
                    return TemplateType.RIVAL
                elif "ajur" in file_path.lower():
                    print(f"[DEBUG] Found 'ajur' in filename: {file_path}")
                    return TemplateType.AJUR
                elif "микроинвест" in file_path.lower() or "microinvest" in file_path.lower():
                    print(f"[DEBUG] Found 'microinvest' in filename: {file_path}")
                    return TemplateType.MICROINVEST
                elif "бизнес навигатор" in file_path.lower() or "business navigator" in file_path.lower():
                    print(f"[DEBUG] Found 'business navigator' in filename: {file_path}")
                    return TemplateType.BUSINESS_NAVIGATOR
                elif "универсум" in file_path.lower() or "universum" in file_path.lower():
                    print(f"[DEBUG] Found 'universum' in filename: {file_path}")
                    return TemplateType.UNIVERSUM
        except Exception as e:
            print(f"[DEBUG] Error checking filename: {e}")
        
        # Quick scan for "ХРОНОЛОГИЧЕН ОПИС" or similar which is indicative of Rival
        try:
            for i in range(min(10, len(df))):
                row_text = ' '.join([str(val).lower() for val in df.iloc[i].values if not pd.isna(val)])
                if "хронологичен опис" in row_text:
                    print(f"[DEBUG] Found 'ХРОНОЛОГИЧЕН ОПИС' in row {i}, likely Rival")
                    # Don't immediately return - check if strong Business Navigator indicators are present
                    if self._has_business_navigator_strong_indicators(headers):
                        print("[DEBUG] Found Business Navigator strong indicators, overriding 'ХРОНОЛОГИЧЕН ОПИС' match")
                        break
                    return TemplateType.RIVAL
        except Exception as e:
            print(f"[DEBUG] Error in quick scan: {e}")
        
        # First check if Business Navigator has strong indicators
        # This changes the detection order to prioritize Business Navigator in ambiguous cases
        print("[DEBUG] Pre-checking Business Navigator strong indicators")
        if self._has_business_navigator_strong_indicators(headers):
            print("[DEBUG] Found strong Business Navigator indicators, checking full pattern")
            if self._check_business_navigator_pattern(df, headers):
                print("[DEBUG] Confirmed Business Navigator pattern")
                return TemplateType.BUSINESS_NAVIGATOR
        
        # Modified detection order - check AJUR first, then Rival
        # This prevents misclassification of AJUR as Rival
        print("[DEBUG] Checking Ajur pattern first")
        if self._check_ajur_pattern(df, headers):
            print("[DEBUG] Matched Ajur pattern")
            return TemplateType.AJUR
        
        print("[DEBUG] Checking Rival pattern")
        if self._check_rival_pattern(df, headers):
            print("[DEBUG] Matched Rival pattern")
            return TemplateType.RIVAL
        
        print("[DEBUG] Checking Microinvest pattern")
        if self._check_microinvest_pattern(df, headers):
            print("[DEBUG] Matched Microinvest pattern")
            return TemplateType.MICROINVEST
        
        # If Business Navigator wasn't detected in pre-check, try again with full check
        print("[DEBUG] Checking Business Navigator pattern")
        if self._check_business_navigator_pattern(df, headers):
            print("[DEBUG] Matched Business Navigator pattern")
            return TemplateType.BUSINESS_NAVIGATOR
        
        print("[DEBUG] Checking Universum pattern")
        if self._check_universum_pattern(df, headers):
            print("[DEBUG] Matched Universum pattern")
            return TemplateType.UNIVERSUM
        
        print("[DEBUG] No template pattern matched")
        return None
    
    def _get_headers(self, df: pd.DataFrame) -> List[str]:
        """Convert DataFrame headers to lowercase strings"""
        return [str(col).lower() for col in df.columns]
    
    def _has_business_navigator_strong_indicators(self, headers: List[str]) -> bool:
        """
        Check if there are strong indicators of Business Navigator template
        Used as a pre-check to differentiate from Rival in ambiguous cases
        """
        # Strong indicators unique to Business Navigator
        bn_unique_indicators = [
            "документ: тип", "документ: номер", "документ: дата",
            "счетоводен текст", "кореспонденция", "контрагент:",
            "име на дилър", "име на партида", "чужд език"
        ]
        
        match_count = 0
        for indicator in bn_unique_indicators:
            if any(indicator.lower() in h.lower() for h in headers):
                match_count += 1
                print(f"[DEBUG] Found strong Business Navigator indicator: '{indicator}'")
        
        # Require at least 2 unique indicators
        return match_count >= 2
    
    def _check_rival_pattern(self, df: pd.DataFrame, headers: List[str]) -> bool:
        """
        Check if the file matches the Rival template pattern
        
        Rival structure:
        - Rows 1, 2, 4, 5, 6 have merged cells from column A to K (header information)
        - Rows 8 and 9 are merged and form the header of the data table
        - Actual data starts at row 10
        - Column structure: вид документ, номер на документ, дата, име, дебит, кредит, сума, обяснение
        
        Note: Rival template typically has ~7-9 rows of metadata before the actual table header
        This is specific to Rival templates only - we don't skip rows for other template types
        """
        print("[DEBUG] Inside _check_rival_pattern - looking for header row")
        
        # Debug output: Show the head of the read file
        print("[DEBUG] DataFrame head (first 5 rows):")
        print(df.head(5).to_string())
        
        # RIVAL-SPECIFIC CHECK: First check if this file is likely an AJUR file to avoid false positives
        # These are strong AJUR indicators that should never be present in a true Rival file
        ajur_specific_indicators = ["установено при одита", "отклонение", "тествани на контролни действия"]
        for indicator in ajur_specific_indicators:
            for i in range(min(5, len(df))):
                row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
                if any(indicator.lower() in val for val in row_values):
                    print(f"[DEBUG] Found AJUR-specific indicator '{indicator}' in row {i}, not a Rival template")
                    return False
        
        # Check for Business Navigator indicators first - if found, this is not Rival
        if self._has_business_navigator_strong_indicators(headers):
            print("[DEBUG] Found Business Navigator indicators, not a Rival template")
            return False
        
        # RIVAL-SPECIFIC KEYWORDS: More specific to avoid false positives with other templates
        # Updated keywords based on actual Rival template
        expected_keywords = [
            "вид док", "номер документ", "дата документ", "сметка дебит", "сметка кредит",
            "стойност", "обяснение на статия", "хронологичен опис"
        ]
        print(f"[DEBUG] Raw headers being checked: {[str(h) for h in df.columns]}")
        print(f"[DEBUG] Looking for these keywords: {expected_keywords}")
        
        # Specific Rival header patterns observed in real files
        rival_specific_patterns = [
            "вид док", "номер документ", "дата документ", "сметка дебит", "сметка кредит",
            "стойност", "обяснение на статия"
        ]
        
        # First check column headers - stricter matching for Rival
        header_keyword_matches = 0
        for keyword in expected_keywords:
            if any(keyword.lower() in h.lower() for h in headers):
                header_keyword_matches += 1
                print(f"[DEBUG] Found Rival-specific keyword '{keyword}' in headers")
        
        print(f"[DEBUG] Found {header_keyword_matches} Rival-specific keyword matches in headers")
        
        # More strict matching for Rival to avoid false positives with other templates
        if header_keyword_matches >= 3:
            print("[DEBUG] Found strong Rival pattern in original headers")
            return True
            
        # If not found in headers, try to find the actual header row by scanning data rows
        # THIS IS RIVAL-SPECIFIC BEHAVIOR: We only scan for headers beyond row 7 for Rival
        try:
            # For Rival specifically, we expect metadata in rows 1-7 with merged cells
            # The header is typically in rows 8-9 (merged)
            # And data starts at row 10
            max_rows_to_check = min(15, len(df))
            
            print(f"[DEBUG] RIVAL-SPECIFIC: Scanning rows 7-{max_rows_to_check} for potential Rival header (merged cells in rows 8-9)")
            
            # First, check for the merged cell pattern - in Rival files, the header rows
            # typically have fewer non-empty cells than data rows due to merged cells
            if len(df) >= 9:
                merged_pattern_detected = True
                merged_cell_rows = [0, 1, 3, 4, 5]  # 0-indexed rows 1, 2, 4, 5, 6
                
                for i in merged_cell_rows:
                    if i < len(df):
                        non_empty_cells = sum(1 for val in df.iloc[i].values if not pd.isna(val))
                        # If the row has many non-empty cells, it's likely not a merged header row
                        if non_empty_cells > 5:
                            merged_pattern_detected = False
                            break
                
                if merged_pattern_detected:
                    print("[DEBUG] Detected potential Rival merged cell pattern in header rows")
                    # Look specifically at rows 7-8 (index 7-8) which should be the merged header
                    for i in [7, 8]:
                        if i < len(df):
                            row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
                            header_keywords = ["вид", "документ", "номер", "дата", "дебит", "кредит", "сума", "обяснение"]
                            matches = sum(any(keyword in val for keyword in header_keywords) for val in row_values)
                            if matches >= 4:  # If we find at least 4 header keywords
                                print(f"[DEBUG] Found strong Rival header pattern at row {i+1} with merged cell structure")
                                return True
            
            # RIVAL-SPECIFIC: Skip the first 7 rows as they are typically metadata
            # Check rows 7+ to see if they could be a header row
            for i in range(7, max_rows_to_check):
                row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
                raw_row_values = [str(val) for val in df.iloc[i].values if not pd.isna(val)]
                
                # Debug the row we're checking with raw values
                print(f"[DEBUG] Checking row {i}: {row_values}")
                
                # 1. Check for individual keywords in row values
                matches = []
                for keyword in expected_keywords:
                    for val in row_values:
                        if keyword.lower() in val.lower():
                            matches.append(keyword)
                            break
                
                print(f"[DEBUG] Row {i} matches: {matches}")
                
                # 2. Special check for exact Rival pattern seen in the debug output
                rival_specific_matches = []
                for pattern in rival_specific_patterns:
                    for val in row_values:
                        if pattern.lower() in val.lower():
                            rival_specific_matches.append(pattern)
                            break
                
                # Increase required matches from 3 to 4
                if len(rival_specific_matches) >= 4:
                    print(f"[DEBUG] Found specific Rival patterns in row {i}: {rival_specific_matches}")
                    return True
                
                # 3. Direct check for the exact patterns observed in real Rival files
                if any("вид" in val and "док" in val for val in row_values) and \
                   any("номер" in val and "документ" in val for val in row_values) and \
                   any("дата" in val and "документ" in val for val in row_values) and \
                   any(("сметка" in val and "дебит" in val) or ("сметка" in val and "кредит" in val) for val in row_values):
                    print(f"[DEBUG] Found direct match for Rival header pattern in row {i}")
                    # Check that it doesn't also have Business Navigator patterns
                    if not any("кореспонденция" in val for val in row_values) and \
                       not any("контрагент:" in val for val in row_values):
                        return True
                    else:
                        print("[DEBUG] Found Business Navigator patterns, not a Rival template")
                
                # Increase required matches from 3 to 5
                if len(matches) >= 5:
                    print(f"[DEBUG] Found potential Rival header at row {i} with {len(matches)} keyword matches")
                    # Check that it doesn't also have Business Navigator specific patterns
                    if not any("кореспонденция" in val for val in row_values) and \
                       not any("контрагент:" in val for val in row_values) and \
                       not any("чужд език" in val for val in row_values):
                        return True
                    else:
                        print("[DEBUG] Found Business Navigator patterns in row, not considering as Rival")
                
                # RIVAL-SPECIFIC: Check if there's a "ХРОНОЛОГИЧЕН ОПИС" or similar phrase
                # which is commonly found in Rival files
                if any("хронологичен" in val.lower() and "опис" in val.lower() for val in row_values):
                    print(f"[DEBUG] Found 'ХРОНОЛОГИЧЕН ОПИС' pattern in row {i}")
                    # If we found this, look ahead a few rows for header
                    ahead_limit = min(i + 10, len(df))
                    # RIVAL-SPECIFIC: Look ahead for the header pattern
                    for j in range(i + 1, ahead_limit):
                        ahead_row = [str(val).lower() for val in df.iloc[j].values if not pd.isna(val)]
                        if any("вид" in val for val in ahead_row) and any("документ" in val for val in ahead_row):
                            print(f"[DEBUG] Found Rival header pattern in row {j} after 'ХРОНОЛОГИЧЕН ОПИС'")
                            return True
            
            print("[DEBUG] No Rival header pattern found in any row")
            return False
        except Exception as e:
            print(f"[ERROR] Error in _check_rival_pattern: {e}")
            return False
    
    def _check_ajur_pattern(self, df: pd.DataFrame, headers: List[str]) -> bool:
        """
        Check if the file matches the AJUR template pattern
        
        AJUR:
        - Common headers: Потр., Опер. No, Дата рег., Вид док., Документ No / дата, Рег. No,
          Дт с/ка, Аналитична сметка, Кт с/ка, Аналитична сметка, Сума, Обяснителен текст
        - Distinctive structure: Each row is a complete accounting operation
        
        Note: Unlike Rival, we don't skip header rows for AJUR templates
        """
        print("[DEBUG] Inside _check_ajur_pattern")
        
        # AJUR-SPECIFIC STRONG INDICATORS - these are highly distinctive for AJUR
        strong_ajur_indicators = [
            "установено при одита",
            "отклонение",
            "тествани на контролни действия",
            "установено наличие на контролно действие при одита",
            "потр.",
            "опер. no",
            "дата рег.",
            "обяснителен текст на друг език"
        ]
        
        # Check headers first for distinctive AJUR patterns
        ajur_header_patterns = [
            "потр.", "опер. no", "дата рег.", "вид док", "документ no / дата", "рег. no",
            "дт с/ка", "кт с/ка", "аналитична сметка", "сума", "обяснителен текст"
        ]
        
        header_matches = 0
        for pattern in ajur_header_patterns:
            for header in headers:
                if pattern in header.lower():
                    print(f"[DEBUG] Found AJUR header pattern '{pattern}' in '{header}'")
                    header_matches += 1
                    break
        
        print(f"[DEBUG] Found {header_matches} AJUR header patterns")
        
        # If we have a significant number of header matches (at least 5), this is likely AJUR
        if header_matches >= 5:
            print(f"[DEBUG] Detected AJUR by header patterns: {header_matches} matches")
            return True
        
        # Check first 5 rows for any of the strong indicators
        for i in range(min(5, len(df))):
            row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
            row_text = ' '.join(row_values)
            for indicator in strong_ajur_indicators:
                if indicator.lower() in row_text:
                    print(f"[DEBUG] Found strong AJUR-specific indicator '{indicator}' in row {i}")
                    # If we find ANY strong AJUR indicator, this is a high-confidence match
                    # But still check a few more keywords to be extra sure
                    secondary_keywords = ["аналитична сметка", "дт с/ка", "кт с/ка", "обяснителен текст"]
                    for keyword in secondary_keywords:
                        if any(keyword.lower() in h.lower() for h in headers) or any(keyword.lower() in val for val in row_values):
                            print(f"[DEBUG] Found confirming AJUR keyword '{keyword}'")
                            return True
        
        # Check for specific column pattern seen in the sample file
        # The sample has a very specific column structure
        if len(df.columns) >= 25:  # AJUR typically has many columns (25+ in sample file)
            # Look for amount column - in sample file it's at position 24
            amount_col_idx = 24 if len(df.columns) > 24 else len(df.columns) - 1
            description_col_idx = 25 if len(df.columns) > 25 else len(df.columns) - 1
            
            # Examine a few rows to see if they match AJUR pattern
            for i in range(min(5, len(df))):
                try:
                    # Check if we have numeric amounts in the expected column
                    amount_val = df.iloc[i, amount_col_idx]
                    if isinstance(amount_val, (int, float)) and not pd.isna(amount_val) and amount_val > 0:
                        # Also check if we have account numbers in expected columns
                        debit_col_idx = 6  # Дт с/ка in sample
                        credit_col_idx = 15  # Кт с/ка in sample
                        
                        debit_val = df.iloc[i, debit_col_idx] if debit_col_idx < len(df.columns) else None
                        credit_val = df.iloc[i, credit_col_idx] if credit_col_idx < len(df.columns) else None
                        
                        if (isinstance(debit_val, str) and '/' in debit_val) or \
                           (isinstance(credit_val, str) and '/' in credit_val):
                            print(f"[DEBUG] Found AJUR pattern in data: accounts with '/' format and amount")
                            return True
                except Exception as e:
                    print(f"[DEBUG] Error checking AJUR data pattern: {e}")
                    pass
        
        # If no strong indicators were found, do a more standard check
        # Update expected keywords to match the actual headers (case-insensitive)
        expected_keywords = ["потр",
                          "опер. no",
                          "дата рег",
                          "вид док",
                          "документ no / дата",
                          "рег. no",
                          "дт с/ка",
                          "аналитична сметка",
                          "кт с/ка",
                          "количество",
                          "мярка",
                          "сума",
                          "обяснителен текст",
                          "установено при одита",
                          "отклонение",
                          "тествани на контролни действия",
                          "установено наличие  на контролно действие   при одита",
                          "системна дата",
                          ]
        
        print(f"[DEBUG] Expected Ajur keywords: {expected_keywords}")
        
        # Check for keyword matches with a reduced threshold (8 instead of 16)
        # This makes the detection more flexible
        keyword_matches = self._check_keywords_in_headers(headers, expected_keywords, min_matches=8)
        print(f"[DEBUG] Ajur keyword matches result: {keyword_matches}")
        
        return keyword_matches
    
    def _check_microinvest_pattern(self, df: pd.DataFrame, headers: List[str]) -> bool:
        """
        Check if the file matches the Microinvest template pattern
        
        Микроинвест: дебит с-ка, кредит с-ка, вид документ, дата, номер на док, партньор, основание
        
        Note: Unlike Rival, we don't skip header rows for Microinvest templates
        """
        # Check for specific column patterns in Microinvest template
        expected_keywords = ["контиране",
                             "дата",
                             "дебит сметка",
                             "дебит",
                             "кредит сметка",
                             "кредит",
                             "сума",
                             "док. вид",
                             "док. дата",
                             "документ №",
                             "партньор",
                             "еик/ддс номер",
                             "държава",
                             "основание",
                             "забележка",
                             "втора забележка",
                             "сделка по зддс",
                             "параграф",
                             "месец за експорт",
                             "потребител"]
        return any("еик/ддс номер" in h for h in headers) and \
               self._check_keywords_in_headers(headers, expected_keywords, min_matches=20)
    
    def _check_business_navigator_pattern(self, df: pd.DataFrame, headers: List[str]) -> bool:
        """
        Check if the file matches the Business Navigator template pattern
        
        Бизнес навигатор: документ: тип, документ: номер, документ: дата, счетоводен текст, сума дебит, номер на сметка, име на сметка, etc.
        
        Note: Unlike Rival, we don't skip header rows for Business Navigator templates
        """
        print("[DEBUG] Inside _check_business_navigator_pattern")
        
        # All columns from row 1 of biznesNavigator201Хронологична.xlsx
        expected_keywords = [
            "кореспонденция",
            "сума дебит",
            "сума кредит",
            "сума в чв дебит",
            "сума в чв кредит",
            "чужда валута",
            "количество дебит",
            "количество кредит",
            "цена",
            "мерна единица",
            "документ: тип",
            "документ: номер",
            "документ: дата",
            "счетоводен текст",
            "папка",
            "период",
            "код",
            "субкод",
            "дилър",
            "име на дилър",
            "група дилъри",
            "крайно салдо дебит",
            "крайно салдо кредит",
            "клас на сметка",
            "номер на сметка",
            "име на кор.сметка",
            "име на кор.сметка, чужд език",
            "код на партида",
            "име на партида",
            "име на партида, чужд език",
            "кореспонденция, чужд език",
            "среден дебит оборот",
            "среден кредит оборот",
            "средно дебит количество",
            "средно кредит количество",
            "клас на кор.сметка",
            "номер на кор.сметка",
            "име дилър,чужд език",
            "контрагент: пощенски код",
            "контрагент: населено място",
            "контрагент: адрес",
            "контрагент: допълн.код",
            "контрагент: данъчен номер",
            "контрагент: еик",
            "контрагент: мол",
            "контрагент: банка",
            "контрагент: банкова сметка",
            "контрагент: банков код",
            "код оп",
            "дебит нач.салдо",
            "кредит нач.салдо",
            "ниво"
        ]
        
        # Also include traditional names in case they appear in other variations
        alternative_keywords = [
            "док тип", "док номер", "док дата", "счетоводен текст"
        ]
        
        print(f"[DEBUG] Checking Business Navigator headers: {headers}")
        print(f"[DEBUG] Expected Business Navigator keywords: {expected_keywords}")
        
        # Strong indicators unique to Business Navigator template
        strong_indicators = [
            "счетоводен текст",
            "документ: тип",
            "кореспонденция",
            "контрагент:",
            "чужд език",
            "име на партида"
        ]
        
        # Check for strong indicators
        strong_indicator_count = 0
        for indicator in strong_indicators:
            if any(indicator.lower() in h.lower() for h in headers):
                print(f"[DEBUG] Found strong Business Navigator indicator: '{indicator}'")
                strong_indicator_count += 1
        
        # If we found multiple strong indicators, that's a very good sign for Business Navigator
        if strong_indicator_count >= 2:
            print(f"[DEBUG] Found {strong_indicator_count} strong Business Navigator indicators")
            
            # With multiple strong indicators, we just need a few more keyword matches
            all_keywords = expected_keywords + alternative_keywords
            matches = self._check_keywords_in_headers(headers, all_keywords, min_matches=5)
            
            if matches:
                print("[DEBUG] Confirmed Business Navigator pattern with strong indicators")
                return True
        
        # Check for matches against all keywords - reduced from 50 to 15
        all_keywords = expected_keywords + alternative_keywords
        matches = self._check_keywords_in_headers(headers, all_keywords, min_matches=15)
        
        print(f"[DEBUG] Business Navigator pattern match result: {matches}")
        return matches
    
    def _check_universum_pattern(self, df: pd.DataFrame, headers: List[str]) -> bool:
        """
        Check if the file matches the Universum template pattern
        
        Универсум: columns C, E, F, I, J, K, L, P
        
        Note: Unlike Rival, we don't skip header rows for Universum templates
        """
        # For Universum, we might need to check the actual data instead of just headers
        # since it's identified by column positions rather than names
        # This is a simplified check that would need to be refined based on actual file samples
        if len(headers) >= 16:  # At least 16 columns (P is the 16th column)
            # Look for patterns in actual data rather than headers
            sample_data = df.iloc[0:5].values.flatten()
            sample_data_str = [str(val).lower() for val in sample_data if not pd.isna(val)]
            
            # Check for accounting-related terms in the data
            accounting_terms = ["дебит", "кредит", "сума", "документ", "операция", "счетоводна"]
            matches = sum(any(term in val for term in accounting_terms) for val in sample_data_str)
            return matches >= 3
        return False
    
    def _check_keywords_in_headers(self, headers: List[str], keywords: List[str], min_matches: int) -> bool:
        """
        Check if at least min_matches keywords are found in the headers
        
        Args:
            headers: List of header strings to check
            keywords: List of keywords to look for
            min_matches: Minimum number of matches required
            
        Returns:
            True if enough matches are found, False otherwise
        """
        print(f"[DEBUG] Checking for {min_matches} matches out of {len(keywords)} keywords")
        print(f"[DEBUG] Headers to check: {headers}")
        print(f"[DEBUG] Keywords to find: {keywords}")
        
        matches = 0
        matched_keywords = []
        
        # First try exact matches (case insensitive)
        for keyword in keywords:
            # Make the comparison case-insensitive
            keyword_lower = keyword.lower()
            
            # Try both exact match and partial match
            exact_match = any(keyword_lower == header.lower() for header in headers)
            partial_match = any(keyword_lower in header.lower() for header in headers)
            
            if exact_match:
                print(f"[DEBUG] Found EXACT match for keyword: '{keyword}'")
                matches += 1
                matched_keywords.append(keyword)
            elif partial_match:
                print(f"[DEBUG] Found PARTIAL match for keyword: '{keyword}'")
                matches += 1
                matched_keywords.append(keyword)
        
        print(f"[DEBUG] Found {matches} matches: {matched_keywords}")
        print(f"[DEBUG] Missing keywords: {[k for k in keywords if k not in matched_keywords]}")
        
        result = matches >= min_matches
        print(f"[DEBUG] Keywords match result: {result} ({matches}/{min_matches})")
        
        return result