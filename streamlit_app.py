import streamlit as st
import pandas as pd
import io
from datetime import datetime, timedelta
import re

st.set_page_config(page_title="Comprehensive CSV Processor", page_icon="⚙️", layout="wide")

st.title("⚙️ Comprehensive CSV Processor")
st.markdown("Complete 9-step CSV processing pipeline for property data")

# Define columns to delete
COLUMNS_TO_DELETE = [
    'OWNER_NAME_STD', 'OWNER_TYPE', 'OWNER_OCCUPIED', 'ASSR_LINK_APN1',
    'PROP_ADDRESS', 'PROP_CITY', 'PROP_STATE', 'PROP_ZIP',
    'LAND_SQFT', 'UNITS_NUMBER', 'CENSUS_BLOCK_GROUP', '_SIMPLIFIED'
]

# Define column renames
COLUMN_RENAMES = {
    'OWNER_NAME_1': 'NAME',
    'OWNER_1_FIRST': 'FIRST NAME',
    'OWNER_1_LAST': 'LAST NAME',
    'OWNER_ADDRESS': 'ADDRESS',
    'OWNER_CITY': 'CITY',
    'OWNER_STATE': 'address_1_state',
    'OWNER_ZIP': 'ZIP/POSTAL CODE',
    'SITE_STATE': 'custom.State'
}

# Company keywords to filter out
COMPANY_KEYWORDS = [' llc', ' corp', ' ltd', ' assoc', ' company', ' lp', 'partnership', ' inc']

def apply_title_case(df):
    """Apply title case to all text columns except state columns"""
    state_columns = ['PROP_STATE', 'SITE_STATE', 'OWNER_STATE', 'address_1_state', 'custom.State']
    
    for col in df.columns:
        if df[col].dtype == 'object':  # Text columns
            if col in state_columns:
                # Convert state columns to ALL CAPS, but preserve NaN values
                df[col] = df[col].str.upper()
            else:
                # Apply title case to other text columns, but preserve NaN values
                df[col] = df[col].str.title()
    
    return df

def filter_company_rows(df, owner_name_col='OWNER_NAME_1'):
    """Remove rows containing company keywords"""
    if owner_name_col not in df.columns:
        return df
    
    initial_count = len(df)
    
    # Create a pattern that matches any of the company keywords (case insensitive)
    pattern = '|'.join([re.escape(keyword) for keyword in COMPANY_KEYWORDS])
    
    # Filter out rows that contain any company keywords
    mask = ~df[owner_name_col].astype(str).str.contains(pattern, case=False, na=False)
    df_filtered = df[mask].copy()
    
    removed_count = initial_count - len(df_filtered)
    return df_filtered, removed_count

def filter_recent_transactions(df, date_col='DATE_TRANSFER', years=10):
    """Remove rows with dates in the last N years"""
    if date_col not in df.columns:
        return df, 0
    
    initial_count = len(df)
    
    # Calculate cutoff date (10 years ago)
    cutoff_date = datetime.now() - timedelta(days=years * 365)
    
    # Convert date column to datetime, handling various formats
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Filter out recent transactions
        mask = (df[date_col].isna()) | (df[date_col] < cutoff_date)
        df_filtered = df[mask].copy()
        
        removed_count = initial_count - len(df_filtered)
        return df_filtered, removed_count
        
    except Exception:
        st.warning(f"Could not process dates in {date_col} column")
        return df, 0

# Input for Mail_CallRail codes
st.subheader("📋 Mail_CallRail Configuration")
col1, col2 = st.columns(2)
with col1:
    code1 = st.text_input("Enter first Mail_CallRail code:", placeholder="e.g., ABC123")
with col2:
    code2 = st.text_input("Enter second Mail_CallRail code:", placeholder="e.g., XYZ789")

if not code1 or not code2:
    st.warning("⚠️ Please enter both Mail_CallRail codes before uploading files")

# File upload - multiple files
uploaded_files = st.file_uploader(
    "Choose CSV files to process", 
    type="csv", 
    accept_multiple_files=True,
    help="Upload 2 or more CSV files with identical column structure",
    disabled=not (code1 and code2)
)

if uploaded_files and len(uploaded_files) >= 1 and code1 and code2:
    try:
        st.subheader("📊 Processing Pipeline")
        
        # Step 1: Merge files
        with st.spinner("Step 1: Merging files..."):
            all_dataframes = []
            for uploaded_file in uploaded_files:
                df = pd.read_csv(uploaded_file)
                all_dataframes.append(df)
            
            if len(uploaded_files) > 1:
                merged_df = pd.concat(all_dataframes, ignore_index=True)
                st.success(f"✅ Step 1: Merged {len(uploaded_files)} files → {len(merged_df):,} total rows")
            else:
                merged_df = all_dataframes[0]
                st.success(f"✅ Step 1: Loaded single file → {len(merged_df):,} rows")
        
        # Step 2: Delete columns
        with st.spinner("Step 2: Deleting columns..."):
            existing_cols_to_delete = [col for col in COLUMNS_TO_DELETE if col in merged_df.columns]
            merged_df = merged_df.drop(columns=existing_cols_to_delete)
            st.success(f"✅ Step 2: Deleted {len(existing_cols_to_delete)} columns → {len(merged_df.columns)} remaining")
        
        # Step 3: Add new columns
        with st.spinner("Step 3: Adding new columns..."):
            # Add Mail_CallRail with alternating values
            mail_callrail_values = [code1 if i % 2 == 0 else code2 for i in range(len(merged_df))]
            merged_df['Mail_CallRail'] = mail_callrail_values
            
            # Add other columns
            merged_df['Lead_Type'] = 'Basic'
            merged_df['Mail_Type'] = 'Neutral Postcard'
            
            st.success("✅ Step 3: Added 3 new columns (Mail_CallRail, Lead_Type, Mail_Type)")
        
        # Step 4: Deduplicate
        with st.spinner("Step 4: Removing duplicates..."):
            initial_count = len(merged_df)
            if 'AGGR_GROUP' in merged_df.columns:
                merged_df = merged_df.drop_duplicates(subset=['AGGR_GROUP'])
                removed_dupes = initial_count - len(merged_df)
                st.success(f"✅ Step 4: Removed {removed_dupes:,} duplicates based on AGGR_GROUP → {len(merged_df):,} rows")
            else:
                st.warning("⚠️ Step 4: AGGR_GROUP column not found, skipping deduplication")
        
        # Step 5: Apply title case
        with st.spinner("Step 5: Applying proper capitalization..."):
            merged_df = apply_title_case(merged_df)
            st.success("✅ Step 5: Applied Title Case (with ALL CAPS for state columns)")
        
        # Step 6: Filter company rows
        with st.spinner("Step 6: Filtering out company records..."):
            merged_df, company_removed = filter_company_rows(merged_df)
            st.success(f"✅ Step 6: Removed {company_removed:,} company records → {len(merged_df):,} rows")
        
        # Step 7: Filter recent transactions
        with st.spinner("Step 7: Filtering recent transactions..."):
            merged_df, recent_removed = filter_recent_transactions(merged_df)
            st.success(f"✅ Step 7: Removed {recent_removed:,} recent transactions → {len(merged_df):,} rows")
        
        # Step 8: Rename columns
        with st.spinner("Step 8: Renaming column headers..."):
            existing_renames = {old: new for old, new in COLUMN_RENAMES.items() if old in merged_df.columns}
            merged_df = merged_df.rename(columns=existing_renames)
            st.success(f"✅ Step 8: Renamed {len(existing_renames)} column headers")
        
        # Step 9: Show alternating pattern info
        st.success(f"✅ Step 9: Mail_CallRail alternates between '{code1}' and '{code2}'")
        
        # Show final results
        st.subheader("📋 Final Results Preview")
        st.dataframe(merged_df.head(10), use_container_width=True)
        
        # Download section
        st.subheader("📥 Download Processed File")
        
        # File naming with date/time
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"processed_property_data_{current_time}.csv"
        filename = st.text_input("Output filename:", value=default_name)
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Convert to CSV
        csv_data = merged_df.to_csv(index=False)
        
        # Download button
        st.download_button(
            label=f"📄 Download {filename}",
            data=csv_data,
            file_name=filename,
            mime="text/csv"
        )
        
        # Final summary
        with st.expander("📊 Complete Processing Summary"):
            st.write("### Processing Results")
            st.write(f"• **Files processed:** {len(uploaded_files)}")
            st.write(f"• **Final row count:** {len(merged_df):,}")
            st.write(f"• **Final column count:** {len(merged_df.columns)}")
            st.write(f"• **Company records removed:** {company_removed:,}")
            st.write(f"• **Recent transactions removed:** {recent_removed:,}")
            
            st.write("### Mail_CallRail Pattern")
            st.write(f"• **Code 1:** {code1}")
            st.write(f"• **Code 2:** {code2}")
            st.write(f"• **Pattern:** Alternates every row")
            
            st.write("### New Columns Added")
            st.write("• **Mail_CallRail:** Alternating codes")
            st.write("• **Lead_Type:** Basic")
            st.write("• **Mail_Type:** Neutral Postcard")
            
            st.write("### Final Column Names")
            for i, col in enumerate(sorted(merged_df.columns), 1):
                st.write(f"{i}. {col}")
        
    except Exception as e:
        st.error(f"Error processing files: {str(e)}")
        st.write("Please make sure all uploaded files are valid CSV files with the expected structure.")

elif uploaded_files and not (code1 and code2):
    st.warning("⚠️ Please enter both Mail_CallRail codes first")

else:
    st.info("👆 Enter Mail_CallRail codes and upload your CSV files to start processing")
    
    # Instructions
    st.markdown("### 🔧 Complete Processing Pipeline:")
    st.markdown("""
    1. **Merge** multiple CSV files (append/stack)
    2. **Delete** 12 specific columns
    3. **Add** 3 new columns (Mail_CallRail with alternating codes, Lead_Type, Mail_Type)
    4. **Deduplicate** based on AGGR_GROUP column
    5. **Proper capitalization** (Title Case + ALL CAPS for states)
    6. **Filter companies** (remove LLC, Corp, Ltd, etc.)
    7. **Filter recent transactions** (remove last 10 years)
    8. **Rename headers** (8 specific column name changes)
    9. **Mail_CallRail alternation** between your two custom codes
    
    **All operations are fully automated** - just enter codes, upload files, and download!
    """)