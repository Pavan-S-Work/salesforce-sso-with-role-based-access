# ──────────────────── Event Loop Bootstrap ──────────────────── #
import asyncio
try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
# ────────────────────────────────────────────────────────────── #

import streamlit as st
import time
import requests
import pandas as pd
import json
import re
from streamlit_oauth import OAuth2Component
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from snowflake.snowpark.exceptions import SnowparkSQLException

# =================== Page Config ==================== #
st.set_page_config(page_title="Salesforce SSO Chat Assistant", page_icon="🔐", layout="wide")

# =================== Configuration =================== #
SALESFORCE_YAML_CONFIG = "SALESFORCEDB.PUBLIC.SALESFORCE_STAGE/salesforceyaml.yaml"

# Salesforce date/timestamp field patterns
SALESFORCE_DATE_FIELDS = [
    'END_DATE', 'START_DATE', 'CLOSE_DATE', 'CREATED_DATE', 'LAST_MODIFIED_DATE',
    'LAST_ACTIVITY_DATE', 'LAST_VIEWED_DATE', 'LAST_REFERENCED_DATE',
    'SYSTEM_MODSTAMP', 'BIRTHDAY', 'DUE_DATE', 'REMIND_DATE', 'ACTIVITY_DATE',
    'COMPLETION_DATE', 'EXPIRATION_DATE', 'EFFECTIVE_DATE', 'ENROLLMENT_DATE'
]

# Initialize Snowflake connection
try:
    cnx = st.connection("snowflake")
    session = cnx.session()
    SNOWFLAKE_AVAILABLE = True
except Exception as e:
    st.error(f"Snowflake connection failed: {e}")
    SNOWFLAKE_AVAILABLE = False

# =================== Load Secrets =================== #
try:
    SF_OAUTH = st.secrets["oauth"]["salesforce"]
    CLIENT_ID = SF_OAUTH["CLIENT_ID"]
    CLIENT_SECRET = SF_OAUTH["CLIENT_SECRET"]
    AUTHORIZE_URL = SF_OAUTH["AUTHORIZE_URL"]
    TOKEN_URL = SF_OAUTH["TOKEN_URL"]
    REFRESH_TOKEN_URL = SF_OAUTH["REFRESH_TOKEN_URL"]
    REVOKE_TOKEN_URL = SF_OAUTH["REVOKE_TOKEN_URL"]
    REDIRECT_URI = SF_OAUTH["REDIRECT_URI"]
    SCOPE = SF_OAUTH.get("SCOPE", "openid profile email api refresh_token")
except KeyError as e:
    st.error(f"Missing required secret: {e}")
    st.stop()

# =================== OAuth Component =================== #
@st.cache_resource
def init_oauth():
    return OAuth2Component(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        authorize_endpoint=AUTHORIZE_URL,
        token_endpoint=TOKEN_URL,
        refresh_token_endpoint=REFRESH_TOKEN_URL,
        revoke_token_endpoint=REVOKE_TOKEN_URL
    )

oauth2 = init_oauth()

# =================== Authentication Helpers =================== #
def normalize_token_expiry(token):
    """Guarantee a usable `expires_at` timestamp on the token."""
    if not token:
        return token

    if 'expires_in' in token:
        token['expires_at'] = time.time() + int(token['expires_in'])
    elif not token.get('expires_at'):
        token['expires_at'] = time.time() + 3600

    return token

def is_token_expired(token):
    if not token:
        return True

    expires_at = token.get('expires_at')
    if not expires_at:
        return True

    return time.time() > (expires_at - 300)

def get_user_info(access_token):
    """Get user information from Salesforce."""
    token_data = st.session_state.get('token', {})
    id_url = token_data.get('id')
    if not id_url:
        st.error("No identity URL in token.")
        return None
    
    resp = requests.get(id_url, headers={'Authorization': f'Bearer {access_token}'})
    if resp.status_code == 200:
        return resp.json()
    st.error(f"UserInfo error: {resp.status_code}")
    return None

def get_full_user_details(access_token, user_id):
    """Fetch Id, Name, Role Name, and Profile Name for the logged-in user"""
    soql = (
        "SELECT Id, Name, UserRole.Name, Profile.Name "
        f"FROM User WHERE Id = '{user_id}'"
    )
    ui = get_user_info(access_token)
    if not ui:
        return None
    instance_url = ui.get('urls', {}).get('rest','').replace('/services/data/v{version}/','')
    if not instance_url:
        st.error("Could not determine instance URL.")
        return None
    
    resp = requests.get(
        f"{instance_url}/services/data/v57.0/query",
        headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
        params={'q': soql}
    )
    if resp.status_code == 200:
        result = resp.json()
        if result.get("records"):
            return result["records"][0]
    return None

# =================== Chat Assistant Functions =================== #
def convert_salesforce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Salesforce timestamp fields from Unix milliseconds to readable dates."""
    if df is None or df.empty:
        return df
    
    df_copy = df.copy()
    
    for col in df_copy.columns:
        col_upper = col.upper()
        if any(date_field in col_upper for date_field in SALESFORCE_DATE_FIELDS):
            try:
                def convert_timestamp(value):
                    if pd.isna(value) or value is None or value == 'None':
                        return None
                    
                    try:
                        timestamp_ms = int(float(value))
                        if 0 < timestamp_ms < 4102444800000:
                            timestamp_s = timestamp_ms / 1000
                            return datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            return value
                    except (ValueError, TypeError, OSError):
                        return value
                
                df_copy[col] = df_copy[col].apply(convert_timestamp)
                
            except Exception as e:
                st.warning(f"Could not convert timestamps in column '{col}': {str(e)}")
                continue
    
    return df_copy

def detect_and_convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Detect and convert potential timestamp columns."""
    if df is None or df.empty:
        return df
    
    df_copy = df.copy()
    
    for col in df_copy.columns:
        col_upper = col.upper()
        if any(date_field in col_upper for date_field in SALESFORCE_DATE_FIELDS):
            continue
            
        sample_values = df_copy[col].dropna().head(10)
        if sample_values.empty:
            continue
            
        timestamp_pattern = re.compile(r'^\d{13}$')
        timestamp_count = sum(1 for val in sample_values if 
                            isinstance(val, (int, float, str)) and 
                            timestamp_pattern.match(str(val)))
        
        if timestamp_count / len(sample_values) > 0.7:
            try:
                def convert_detected_timestamp(value):
                    if pd.isna(value) or value is None or value == 'None':
                        return None
                    
                    try:
                        timestamp_ms = int(float(value))
                        if 1000000000000 <= timestamp_ms <= 4102444800000:
                            timestamp_s = timestamp_ms / 1000
                            return datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            return value
                    except (ValueError, TypeError, OSError):
                        return value
                
                df_copy[col] = df_copy[col].apply(convert_detected_timestamp)
                st.info(f"🕒 Detected and converted timestamp column: '{col}'")
                
            except Exception as e:
                continue
    
    return df_copy

def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """Send chat history to the Cortex Analyst API via stored procedure."""
    if not SNOWFLAKE_AVAILABLE:
        return {"request_id": "error"}, "❌ Snowflake connection not available"
    
    semantic_model_file = f"@{SALESFORCE_YAML_CONFIG}"
    
    try:
        result = session.call(
            "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_API_PROCEDURE",
            messages,
            semantic_model_file
        )
        
        if result is None:
            return {"request_id": "error"}, "❌ No response from Cortex Analyst procedure"
        
        # Parse response
        if isinstance(result, str):
            response_data = json.loads(result)
        else:
            response_data = result
        
        # Handle successful response
        if response_data.get("success", False):
            return_data = {
                "message": response_data.get("analyst_response", {}),
                "request_id": response_data.get("request_id", "N/A"),
                "warnings": response_data.get("warnings", [])
            }
            return return_data, None
        
        # Handle error response
        error_details = response_data.get("error_details", {})
        error_msg = f"""
❌ **Cortex Analyst Error**

**Error Code:** `{error_details.get('error_code', 'N/A')}`  
**Request ID:** `{error_details.get('request_id', 'N/A')}`  
**Status:** `{error_details.get('response_code', 'N/A')}`

**Message:** {error_details.get('error_message', 'No error message provided')}

💡 **Troubleshooting:**
- Verify your salesforce.yaml file exists in the stage
- Check database and schema permissions
- Ensure Cortex Analyst is properly configured
        """
        
        return_data = {
            "request_id": response_data.get("request_id", "error"),
            "warnings": response_data.get("warnings", [])
        }
        return return_data, error_msg
        
    except SnowparkSQLException as e:
        error_msg = f"""
❌ **Database Error**

{str(e)}

💡 **Check:**
- Procedure exists
- You have EXECUTE permissions
- YAML file exists in stage
        """
        return {"request_id": "error"}, error_msg
        
    except Exception as e:
        error_msg = f"❌ **Unexpected Error:** {str(e)}"
        return {"request_id": "error"}, error_msg

def process_salesforce_query(sql_query: str, user_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Process and modify SQL query for Salesforce with user ID filtering."""
    if not SNOWFLAKE_AVAILABLE:
        return None, "❌ Snowflake connection not available"
    
    try:
        result = session.call(
            "CORTEX_ANALYST.CORTEX_AI.SALESFORCE_QUERY_PROCESSOR",
            sql_query,
            user_id
        )
        
        if result is None:
            return None, "❌ No response from query processor"
        
        # Parse response
        if isinstance(result, str):
            response_data = json.loads(result)
        else:
            response_data = result
        
        # Handle successful response
        if response_data.get("success", False):
            return response_data.get("modified_query"), None
        
        # Handle error response
        error_msg = f"""
❌ **Query Processing Error**

**Error Code:** `{response_data.get('error_code', 'N/A')}`  
**Message:** {response_data.get('error_message', 'No error message provided')}
        """
        return None, error_msg
        
    except Exception as e:
        error_msg = f"❌ **Query Processing Error:** {str(e)}"
        return None, error_msg

@st.cache_data(show_spinner=False, ttl=300)
def execute_data_procedure(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Execute data procedure with caching and optimized error handling."""
    if not SNOWFLAKE_AVAILABLE:
        return None, "❌ Snowflake connection not available"
    
    try:
        df = session.call(
            "SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.DREMIO_DATA_PROCEDURE",
            query
        ).collect()
        
        if df:
            # Convert to pandas DataFrame
            df = pd.DataFrame(df)
            
            # Convert timestamps for Salesforce data
            if not df.empty:
                df = convert_salesforce_timestamps(df)
                df = detect_and_convert_timestamps(df)
        
        return df, None
        
    except SnowparkSQLException as e:
        error_str = str(e).lower()
        
        if any(pattern in error_str for pattern in [
            "syntax error", "unexpected 'month'", "unexpected 'year'",
            "unexpected 'day'", "invalid date", "data not available"
        ]):
            return None, "⚠️ Data not available. Please contact your administrator."
        elif "does not exist" in error_str:
            error_msg = f"❌ **Salesforce Procedure Not Found**\n\nVerify the procedure exists and you have access."
        elif "access denied" in error_str or "insufficient privileges" in error_str:
            error_msg = f"❌ **Permission Denied**\n\nInsufficient privileges for Salesforce procedure."
        else:
            error_msg = f"❌ **Salesforce Error:** {str(e)}"
            
        return None, error_msg
        
    except Exception as e:
        return None, f"❌ **Unexpected Error:** {str(e)}"

# =================== UI Functions =================== #
def reset_chat_state():
    """Reset chat-related session state."""
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    if "initial_question_asked" in st.session_state:
        del st.session_state.initial_question_asked

def show_authenticated_interface(token):
    """Show the main authenticated interface with chat assistant."""
    access = token.get('access_token')
    ui = get_user_info(access)
    
    # Initialize chat session state
    if "messages" not in st.session_state:
        reset_chat_state()

    # ───────── Sidebar ───────── #
    with st.sidebar:
        st.title("🔐 User Info")
        if ui:
            st.success("✅ Authenticated")
            user_id = ui.get('user_id')
            st.write(f"**Display Name:** {ui.get('display_name')}")
            
            if user_id:
                user_details = get_full_user_details(access, user_id)
                if user_details:
                    st.write(f"**User ID:** {user_details.get('Id')}")
                    st.write(f"**Name:** {user_details.get('Name')}")
                    st.write(f"**Role:** {user_details.get('UserRole', {}).get('Name', 'N/A')}")
                    st.write(f"**Profile:** {user_details.get('Profile', {}).get('Name', 'N/A')}")
                    
                    # Store user_id for chat assistant
                    st.session_state.salesforce_user_id = user_details.get('Id')
        
        st.divider()
        
        # Data source info
        st.subheader("📊 Data Source")
        st.info("Using **Salesforce** data source")
        
        st.divider()
        
        if st.button("🚪 Logout", type="primary", use_container_width=True):
            oauth2.revoke_token(token)
            del st.session_state['token']
            reset_chat_state()
            st.success("Logged out successfully!")
            st.rerun()

    # ───────── Main Chat Interface ───────── #
    st.title("💬 Salesforce AI Data Assistant")
    st.markdown("Ask questions about your **Salesforce** data using natural language.")
    
    # Show initial question only once
    if (len(st.session_state.messages) == 0 and 
        "initial_question_asked" not in st.session_state):
        st.session_state.initial_question_asked = True
        process_user_input("What questions can I ask?", ui)
    
    # Display conversation
    display_conversation()
    
    # Handle user input
    handle_user_inputs(ui)
    
    # Handle error notifications
    handle_error_notifications()

def display_conversation():
    """Display the conversation history."""
    for idx, message in enumerate(st.session_state.messages):
        if message.get("hidden", False):
            continue
            
        role = message["role"]
        content = message["content"]
        
        with st.chat_message(role):
            display_message(content, idx)

def display_message(content: List[Dict[str, Union[str, Dict]]], message_index: int):
    """Display a single message content."""
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            st.markdown("**💡 Suggested questions:**")
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, 
                    key=f"suggestion_{message_index}_{suggestion_index}",
                    type="secondary"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql_query(item["statement"], message_index, item.get("confidence"))

def display_sql_query(sql: str, message_index: int, confidence: dict):
    """Display SQL query and execute it."""
    user_id = st.session_state.get('salesforce_user_id')
    
    # Process the query first
    with st.spinner("🔄 Processing query for your access level..."):
        processed_query, process_error = process_salesforce_query(sql, user_id)
        
        if process_error:
            st.error(process_error)
            return
            
        if not processed_query:
            st.error("❌ Failed to process query")
            return

    # Execute and display results
    with st.expander("📊 Query Results", expanded=True):
        with st.spinner("⚡ Executing query on Salesforce..."):
            df, err_msg = execute_data_procedure(processed_query)
            
            if df is None:
                if "Data not available" in err_msg:
                    st.warning("⚠️ **No Data Available** - The requested data is not available in the system.")
                else:
                    st.error(err_msg)
            elif df.empty:
                st.warning("📭 **No Records Found** - Your query executed successfully but returned no data.")
            else:
                # Display results in tabs
                data_tab, chart_tab = st.tabs(["📄 Data", "📈 Chart"])
                
                with data_tab:
                    st.dataframe(df, use_container_width=True)
                    st.caption(f"📊 {len(df)} rows returned")

                with chart_tab:
                    display_charts_tab(df, message_index)

def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """Display charts tab."""
    if len(df.columns) < 2:
        st.info("📊 At least 2 columns required for charts")
        return
    
    all_cols = df.columns.tolist()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        x_col = st.selectbox("X-axis", all_cols, key=f"x_col_select_{message_index}")
    
    with col2:
        available_y_cols = [col for col in all_cols if col != x_col]
        y_col = st.selectbox("Y-axis", available_y_cols, key=f"y_col_select_{message_index}")
    
    with col3:
        chart_type = st.selectbox(
            "Chart type",
            ["📈 Line", "📊 Bar", "🔢 Area"],
            key=f"chart_type_{message_index}"
        )
    
    # Create chart based on selection
    try:
        chart_data = df.set_index(x_col)[y_col]
        
        if chart_type == "📈 Line":
            st.line_chart(chart_data)
        elif chart_type == "📊 Bar":
            st.bar_chart(chart_data)
        elif chart_type == "🔢 Area":
            st.area_chart(chart_data)
            
    except Exception as e:
        st.error(f"❌ Chart error: {str(e)}")

def handle_user_inputs(user_info):
    """Handle user inputs from the chat interface."""
    user_input = st.chat_input("Ask me anything about your Salesforce data...")
    if user_input:
        process_user_input(user_input, user_info)
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion, user_info)

def process_user_input(prompt: str, user_info):
    """Process user input and update the conversation history."""
    # Clear previous warnings
    st.session_state.warnings = []

    # Create user message (hidden from UI)
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
        "hidden": True
    }
    st.session_state.messages.append(new_user_message)
    
    # Prepare messages for API
    messages_for_api = [
        {"role": msg["role"], "content": msg["content"]}
        for msg in st.session_state.messages
    ]

    # Show analyst response with progress
    with st.chat_message("analyst"):
        with st.spinner("🤔 Analyzing your Salesforce data..."):
            response, error_msg = get_analyst_response(messages_for_api)
            
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response.get("request_id", "error"),
                }
                st.session_state["fire_API_error_notify"] = True

            if "warnings" in response:
                st.session_state.warnings = response["warnings"]

            st.session_state.messages.append(analyst_message)
            st.rerun()

def handle_error_notifications():
    """Handle error notifications."""
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

# =================== Main Application =================== #
def main():
    st.title("🔐 Salesforce SSO Data Assistant")
    st.markdown("---")

    # ─ Login Flow ─
    if 'token' not in st.session_state:
        st.markdown("### Please log in with Salesforce")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            auth = oauth2.authorize_button(
                name="Login with Salesforce",
                redirect_uri=REDIRECT_URI,
                scope=SCOPE,
                key="sf_oauth",
                pkce="S256"
            )
            
            if auth and 'token' in auth:
                token = normalize_token_expiry(auth['token'])
                st.session_state['token'] = token
                st.success("✅ Authentication successful!")
                st.rerun()
        return

    # ─ Token Refresh Flow ─
    token = normalize_token_expiry(st.session_state['token'])
    st.session_state['token'] = token

    if is_token_expired(token):
        st.info("🔄 Refreshing access token...")
        try:
            new_token = oauth2.refresh_token(token)
            new_token = normalize_token_expiry(new_token)
            st.session_state['token'] = new_token
            st.success("✅ Token refreshed successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Token refresh failed: {e}")
            del st.session_state['token']
            st.rerun()
        return

    # ─ Authenticated Interface ─
    show_authenticated_interface(token)

if __name__ == "__main__":
    main()