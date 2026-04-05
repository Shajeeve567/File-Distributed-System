import streamlit as st
import requests
import json

# Configuration
# Default node list from the cluster configuration
NODES = {
    "Node 1 (8001)": "http://127.0.0.1:8001",
    "Node 2 (8002)": "http://127.0.0.1:8002",
    "Node 3 (8003)": "http://127.0.0.1:8003"
}

st.set_page_config(page_title="Distributed File System", page_icon="🗄️", layout="wide")

st.title("🗄️Distributed File System Dashboard")

# --- Sidebar ---
st.sidebar.header("Connection Settings")
selected_node_name = st.sidebar.selectbox("Select Node to Connect", list(NODES.keys()))
base_url = NODES[selected_node_name]
st.sidebar.caption(f"Currently pointing to: `{base_url}`")

# Keep track of active URL in case we get redirected to a leader
if "active_url" not in st.session_state:
    st.session_state.active_url = base_url

# Update active URL if user changes selection
if st.session_state.get("last_selected") != selected_node_name:
    st.session_state.active_url = base_url
    st.session_state.last_selected = selected_node_name

# --- Helper Functions ---
def perform_request(method, endpoint, **kwargs):
    """Executes a request and handles redirecting to the Raft Leader if needed."""
    url = f"{st.session_state.active_url}{endpoint}"
    try:
        r = requests.request(method, url, timeout=5, **kwargs)
        
        # Check for Leader redirection on write operations (POST/DELETE)
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError:
                data = None
                
            if data and isinstance(data, dict) and data.get("status") == "error" and data.get("leader_url"):
                new_url = data["leader_url"]
                st.warning(f"Redirecting to Leader: {new_url}")
                st.session_state.active_url = new_url  # Update session
                
                # Reset file pointers if a file was passed
                if "files" in kwargs:
                    for key in kwargs["files"]:
                        kwargs["files"][key][1].seek(0)
                        
                # Retry with new URL
                retry_url = f"{new_url}{endpoint}"
                return requests.request(method, retry_url, timeout=5, **kwargs)
                
        # If API returns 503 Service Unavailable, it might be due to No Leader Elected yet
        elif r.status_code == 503:
            st.error(f"503 Service Unavailable: {r.text}")
            
        return r
    except requests.exceptions.ConnectionError:
        st.error(f"🔌 Failed to connect to {url}. Is the node running?")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None

# --- Main Tabs ---
tab1, tab2, tab3 = st.tabs(["Dashboard & Metrics", "File Management", "System Details"])

# --- Tab 1: Metrics ---
with tab1:
    st.header("Real-time Metrics")
    
    col_btn, _ = st.columns([1, 5])
    with col_btn:
        if st.button("Refresh Metrics"):
            pass # Reruns the script anyway
            
    metrics_response = perform_request("GET", "/api/metrics")
    
    if metrics_response and metrics_response.status_code == 200:
        data = metrics_response.json()
        
        # Parse subgroups safety
        consensus = data.get("consensus", {})
        replication = data.get("replication", {})
        fault = data.get("fault_tolerance", {})
        time_sync = data.get("time_sync", {})
        
        st.subheader("Consensus (Raft)")
        m1, m2, m3 = st.columns(3)
        m1.metric("Current Leader", consensus.get("leader", "None"), f"Term: {consensus.get('term', 0)}")
        m2.metric("Target Node State", consensus.get("state", "Unknown").upper())
        m3.metric("Vote Count", consensus.get("vote_count", 0))
        
        st.divider()
        st.subheader("Data Replication & Storage")
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Files/Blocks Stored", replication.get("files_stored", 0))
        m2.metric("Connected Peers", replication.get("peer_count", 0))
        m3.write("**Peer List:**")
        st.write(replication.get("peers", []))
        
        st.divider()
        st.subheader("Fault Tolerance")
        m1, m2 = st.columns(2)
        m1.metric("Healthy Nodes", fault.get("healthy_counts", 0))
        m2.metric("Failed Nodes", fault.get("failed_counts", 0))
        
        with st.expander("View Node Status Details"):
            st.json(fault.get("node_status_details", {}))
            
        st.divider()
        st.subheader("Time Synchronization")
        m1, m2 = st.columns(2)
        m1.metric("Lamport Clock Counter", time_sync.get("lamport_counter", 0))
        m2.write(f"Protocol: `{time_sync.get('protocol', 'Unknown')}`")

    elif metrics_response:
        st.warning(f"Could not fetch metrics. HTTP {metrics_response.status_code}: {metrics_response.text}")


# --- Tab 2: File Management ---
with tab2:
    st.header("File Operations")
    
    col_upload, col_download = st.columns(2)
    
    with col_upload:
        st.subheader("Upload File")
        uploaded_file = st.file_uploader("Choose a file to upload")
        upload_filename = st.text_input("Save as (filename)", help="e.g. data.txt")
        
        if st.button("Upload", type="primary"):
            if uploaded_file and upload_filename:
                # Prepare file payload for requests
                # Using a tuple (filename, fileobj) ensures the file pointer can be reset if needed
                files = {"file": (uploaded_file.name, uploaded_file)}
                
                with st.spinner("Uploading, partitioning into blocks and replicating via Raft..."):
                    res = perform_request("POST", f"/files/{upload_filename}", files=files)
                    
                if res and res.status_code == 200:
                    st.success(f"Successfully uploaded `{upload_filename}`")
                    with st.expander("Upload Details"):
                        st.json(res.json())
                elif res:
                    st.error(f"Error uploading file: {res.text}")
            else:
                st.warning("Please provide both a file and a destination filename.")
                
    with col_download:
        st.subheader("Download / Read File")
        download_filename = st.text_input("Enter filename to retrieve")
        
        if st.button("Fetch File"):
            if download_filename:
                with st.spinner("Locating blocks and fetching..."):
                    res = perform_request("GET", f"/files/{download_filename}")
                    
                if res and res.status_code == 200:
                    st.success(f"File retrieved! Size: {len(res.content)} bytes")
                    
                    st.download_button(
                        label="💾 Download File Output",
                        data=res.content,
                        file_name=download_filename,
                        mime="application/octet-stream"
                    )
                    
                    with st.expander("File Content Preview"):
                        try:
                            # Try to decode as text
                            text_content = res.content.decode('utf-8')
                            st.text_area("Content", text_content, height=200)
                        except UnicodeDecodeError:
                            st.info("Binary file detected. Preview not available.")
                elif res:
                    # Parse error details if possible
                    try:
                        err_msg = res.json().get("error", res.text)
                    except:
                        err_msg = res.text
                    st.error(f"Error fetching file: {err_msg}")
            else:
                st.warning("Please enter a filename.")

# --- Tab 3: System Details ---
with tab3:
    st.header("Raw System Status")
    st.write("Comprehensive dump of the backend system architecture status.")
    
    if st.button("Refresh Raw Status"):
        pass
        
    res = perform_request("GET", "/status")
    if res and res.status_code == 200:
        st.json(res.json())
    elif res:
        st.error(f"Failed to fetch status: {res.text}")
