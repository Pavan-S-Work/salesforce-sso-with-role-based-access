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
from streamlit_oauth import OAuth2Component
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
# from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# =================== Page Config ==================== #
st.set_page_config(page_title="Salesforce SSO + Cortex Analyst", page_icon="🔐", layout="wide")

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

# =================== YAML Configs =================== #
YAML_CONFIGS = {
    "Salesforce": "PAVANADMIN.PUBLIC.SALESFORCE_STAGE/salesforceadmin.yaml",
    "Odoo": "ODOO.PUBLIC.ODOO_STAGE/odoo.yaml",
    "SAP": "SAPHANA.PRODUCTSCHEMA.SAPHANA_STAGE/sap.yaml"
}
SALESFORCE_DATE_FIELDS = [
    'END_DATE', 'START_DATE', 'CLOSE_DATE', 'CREATED_DATE', 'LAST_MODIFIED_DATE',
    'LAST_ACTIVITY_DATE', 'LAST_VIEWED_DATE', 'LAST_REFERENCED_DATE',
    'SYSTEM_MODSTAMP', 'BIRTHDAY', 'DUE_DATE', 'REMIND_DATE', 'ACTIVITY_DATE',
    'COMPLETION_DATE', 'EXPIRATION_DATE', 'EFFECTIVE_DATE', 'ENROLLMENT_DATE'
]
cnx = st.connection("snowflake")
session = cnx.session()

# =================== Salesforce Helpers =================== #
def normalize_token_expiry(token):
    if not token:
        return token
    if 'expires_in' in token:
        token['expires_at'] = time.time() + int(token['expires_in'])
    elif not token.get('expires_at'):
        token['expires_at'] = time.time() + 3600
    return token

def is_token_expired(token):
    if not token: return True
    expires_at = token.get('expires_at')
    if not expires_at: return True
    now = time.time()
    return now > (expires_at - 300)

def get_user_info(access_token):
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

# =================== Data Conversion Helpers =================== #
def convert_salesforce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
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

def modify_salesforce_query(sql: str) -> str:
    patterns = [
        (r'("[sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB]")\.("[pP][uU][bB][lL][iI][cC]")\.', r'\1.'),
        (r'\b([sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB])\.([pP][uU][bB][lL][iI][cC])\.', r'\1.'),
        (r'("[sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB]")\.([pP][uU][bB][lL][iI][cC])\.', r'\1.'),
        (r'\b([sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB])\.("[pP][uU][bB][lL][iI][cC]")\.', r'\1.')
    ]
    for pattern, replacement in patterns:
        sql = re.sub(pattern, replacement, sql)
    return sql

# =================== Snowflake Call Helpers =================== #
@st.cache_data(show_spinner=False, ttl=300)
def execute_data_procedure(query: str, data_source: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        if data_source == "Salesforce":
            modified_query = modify_salesforce_query(query)
            st.write(f"Modified_query is-: {modified_query}")
            procedure_call = f"CALL SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.dremio_data_procedure('{modified_query}')"
        elif data_source == "Odoo":
            procedure_call = f"CALL SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.dremio_data_procedure('{query}')"
        elif data_source == "SAP":
            procedure_call = f"CALL SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.dremio_data_procedure('{query}')"
        else:
            return None, f"❌ Unknown data source: {data_source}"
        df = session.sql(procedure_call).to_pandas()
        if data_source == "Salesforce" and df is not None and not df.empty:
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
            error_msg = f"❌ **{data_source} Procedure Not Found**\n\nVerify the procedure exists and you have access."
        elif "access denied" in error_str or "insufficient privileges" in error_str:
            error_msg = f"❌ **Permission Denied**\n\nInsufficient privileges for {data_source} procedure."
        else:
            error_msg = f"❌ **{data_source} Error:** {str(e)}"
        return None, error_msg
    except Exception as e:
        return None, f"❌ **Unexpected Error:** {str(e)}"

# =================== Cortex Analyst API Call (Merged + Salesforce UserId) =================== #
def get_analyst_response(messages: List[Dict], data_source: str, sf_user_id: Optional[str] = None) -> Tuple[Dict, Optional[str]]:
    if data_source == "Salesforce":
        selected_yaml_path = YAML_CONFIGS["Salesforce"]
        semantic_model_file = f"@{selected_yaml_path}"
        # Use new procedure for Salesforce, send CreatedById = sf_user_id along with messages
        try:
            result = session.call(
                "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_SALESFORCE_API_PROCEDURE",
                messages,
                semantic_model_file,
                sf_user_id
            )
        except Exception as e:
            return {"request_id": "error"}, f"❌ **Snowflake Call Error:** {e}"
    else:
        selected_yaml_path = YAML_CONFIGS[data_source]
        semantic_model_file = f"@{selected_yaml_path}"
        try:
            result = session.call(
                "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_API_PROCEDURE",
                messages,
                semantic_model_file
            )
        except Exception as e:
            return {"request_id": "error"}, f"❌ **Snowflake Call Error:** {e}"
    if result is None:
        return {"request_id": "error"}, "❌ No response from Cortex Analyst procedure"
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
- Verify your {data_source.lower()}.yaml file exists in the stage
- Check database and schema permissions
- Ensure Cortex Analyst is properly configured
    """
    return_data = {
        "request_id": response_data.get("request_id", "error"),
        "warnings": response_data.get("warnings", [])
    }
    return return_data, error_msg

# =================== UI =================== #
def main():
    st.title("🔐 Salesforce SSO + Cortex Analyst")
    st.markdown("---")
    # ───────────────── Salesforce Login ───────────────── #
    if 'token' not in st.session_state:
        st.markdown("### Please log in with Salesforce to access the AI Analyst")
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            auth = oauth2.authorize_button(
                name="Login with Salesforce",
                redirect_uri=REDIRECT_URI,
                scope=SCOPE,
                key="sf_oauth",
                pkce="S256"
            )
            if auth and 'token' in auth:
                t = normalize_token_expiry(auth['token'])
                st.session_state['token'] = t
                st.success("✅ Authentication successful")
                st.rerun()
        return

    token = normalize_token_expiry(st.session_state['token'])
    st.session_state['token'] = token
    if is_token_expired(token):
        st.info("🔄 Refreshing access token...")
        try:
            new_t = oauth2.refresh_token(token)
            new_t = normalize_token_expiry(new_t)
            st.session_state['token'] = new_t
            st.success("✅ Token refreshed successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Token refresh failed: {e}")
            del st.session_state['token']
            st.rerun()
        return

    access = token.get('access_token')
    sf_user_info = get_user_info(access)
    user_id = sf_user_info.get('user_id') if sf_user_info else None

    # Sidebar: User Info
    with st.sidebar:
        if sf_user_info:
            st.success("Authenticated")
            st.write(f"Display Name: {sf_user_info.get('display_name')}")
            if user_id:
                user_details = get_full_user_details(access, user_id)
                if user_details:
                    st.write(f"User ID: {user_details.get('Id')}")
                    st.write(f"Name: {user_details.get('Name')}")
                    st.write(f"Role: {user_details.get('UserRole', {}).get('Name', 'N/A')}")
                    st.write(f"Profile: {user_details.get('Profile', {}).get('Name', 'N/A')}")
        if st.button("Logout"):
            oauth2.revoke_token(token)
            del st.session_state['token']
            st.success("Logged out")
            st.rerun()

    # ───────────────── Chat Assistant ───────────────── #
    # Data source selection (Salesforce, Odoo, SAP)
    if "selected_yaml" not in st.session_state:
        st.session_state.selected_yaml = "Salesforce"
    data_source = st.selectbox(
        "Select Data Source:",
        options=list(YAML_CONFIGS.keys()),
        index=list(YAML_CONFIGS.keys()).index(st.session_state.selected_yaml),
        key="yaml_selector"
    )
    if data_source != st.session_state.selected_yaml:
        st.session_state.messages = []
        st.session_state.active_suggestion = None
        st.session_state.warnings = []
        st.session_state.selected_yaml = data_source
        if "initial_question_asked" in st.session_state:
            del st.session_state.initial_question_asked
        st.rerun()
    st.info(f"📊 **{st.session_state.selected_yaml}** data source")
    st.divider()

    # Session state for chat
    if "messages" not in st.session_state:
        reset_session_state()
    # Show initial question only once
    if len(st.session_state.messages) == 0 and st.session_state.selected_yaml and "initial_question_asked" not in st.session_state:
        st.session_state.initial_question_asked = True
        process_user_input("What questions can I ask?", data_source, user_id)
    display_conversation()
    handle_user_inputs(data_source, user_id)
    handle_error_notifications()

def reset_session_state():
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    if "initial_question_asked" in st.session_state:
        del st.session_state.initial_question_asked

def handle_user_inputs(data_source, sf_user_id):
    if not st.session_state.selected_yaml:
        st.warning("Please select a data source first.")
        return
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input, data_source, sf_user_id)
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion, data_source, sf_user_id)

def process_user_input(prompt: str, data_source: str, sf_user_id: Optional[str]):
    st.session_state.warnings = []
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
        "hidden": True
    }
    st.session_state.messages.append(new_user_message)
    messages_for_api = [
        {"role": msg["role"], "content": msg["content"]}
        for msg in st.session_state.messages
    ]
    with st.chat_message("analyst"):
        with st.spinner("🤔 Analyzing your Data..."):
            response, error_msg = get_analyst_response(
                messages_for_api,
                data_source,
                sf_user_id if data_source == "Salesforce" else None
            )
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
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def display_conversation():
    for idx, message in enumerate(st.session_state.messages):
        if message.get("hidden", False):
            continue
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_message(content, idx)

def display_message(content: List[Dict[str, Union[str, Dict]]], message_index: int):
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
            display_sql_query(
                item["statement"], message_index, item.get("confidence")
            )

def display_sql_confidence(confidence: dict):
    if confidence is None:
        return
    verified_query_used = confidence.get("verified_query_used")
    with st.popover("🔍 Verified Query Info", help="Query verification details"):
        if verified_query_used is None:
            return
        st.write(f"**Name:** {verified_query_used.get('name', 'N/A')}")
        st.write(f"**Question:** {verified_query_used.get('question', 'N/A')}")
        st.write(f"**Verified by:** {verified_query_used.get('verified_by', 'N/A')}")
        if 'verified_at' in verified_query_used:
            st.write(f"**Verified at:** {datetime.fromtimestamp(verified_query_used['verified_at'])}")
        with st.expander("SQL Query"):
            st.code(verified_query_used.get("sql", "N/A"), language="sql")

def display_sql_query(sql: str, message_index: int, confidence: dict):
    current_data_source = st.session_state.selected_yaml
    if current_data_source == "Salesforce":
        modified_sql = modify_salesforce_query(sql)
        query_was_modified = sql != modified_sql
    else:
        modified_sql = sql
        query_was_modified = False
    display_sql_confidence(confidence)
    with st.expander("📊 Results", expanded=True):
        with st.spinner(f"⚡ Executing via {current_data_source}..."):
            df, err_msg = execute_data_procedure(sql, current_data_source)
            if df is None:
                if err_msg and "Data not available" in err_msg:
                    st.warning("""
                    ⚠️ **No Data Available**
                    
                    The requested data is not available in the system. 
                    This could be because:
                    - The data hasn't been loaded yet
                    - The time period you requested has no records
                    - The specific records don't exist
                    
                    Please contact your administrator for assistance.
                    """)
                else:
                    st.error(err_msg)
            elif df.empty:
                st.warning("""
                📭 **No Records Found**
                
                Your query executed successfully but returned no data.
                Try adjusting your filters or time period.
                """)
            else:
                data_tab, chart_tab = st.tabs(["📄 Data", "📈 Chart"])
                with data_tab:
                    st.dataframe(df, use_container_width=True)
                    st.caption(f"📊 {len(df)} rows returned")
                with chart_tab:
                    display_charts_tab(df, message_index)

def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    if len(df.columns) < 2:
        st.info("📊 At least 2 columns required for charts")
        return
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    all_cols = df.columns.tolist()
    col1, col2, col3 = st.columns(3)
    with col1:
        x_col = st.selectbox(
            "X-axis", all_cols, 
            key=f"x_col_select_{message_index}"
        )
    with col2:
        available_y_cols = [col for col in all_cols if col != x_col]
        y_col = st.selectbox(
            "Y-axis", available_y_cols,
            key=f"y_col_select_{message_index}"
        )
    with col3:
        chart_type = st.selectbox(
            "Chart type",
            ["📈 Line", "📊 Bar", "🔢 Area"],
            key=f"chart_type_{message_index}"
        )
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

if __name__ == "__main__":
    main()