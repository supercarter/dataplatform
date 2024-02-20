import streamlit as st
from streamlit_file_browser import st_file_browser
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import ast
import scipy


data_lake_location = "../data_lake/"

# Page config
st.set_page_config(
    page_title="HealthStats",
    page_icon="🩸",
    layout="wide",
    menu_items={
        'About': "Data comes from [CDC NHANES](https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2017). Cleaned data and descriptions are [here](https://github.com/RileyZurrin/NHANES_Extractor)."
    }
)
# Set Plotly color scheme
px.defaults.color_discrete_sequence = px.colors.qualitative.Safe


# Initialize session states for variables (this is for swap button to work properly)
if 'v1index' not in st.session_state:
    st.session_state.v1index = None
if 'v2index' not in st.session_state:
    st.session_state.v2index = None

# Function to choose pandas read function based on file type
@st.cache_resource(show_spinner="Loading Data")
def pd_read_file(file_path):
    read_functions = {
        'csv': pd.read_csv,
        'xls': pd.read_excel,
        'xlsx': pd.read_excel,
        'parquet': pd.read_parquet,
        # Add more file formats as needed
    }
    file_type = file_path.split('.')[-1]

    if file_type not in read_functions:
        raise ValueError(f"File type {file_type} not supported. Please use one of {list(read_functions.keys())}")

    read_function = read_functions[file_type]

    return read_function(data_lake_location + file_path)

# Cache so data is only read once
@st.cache_data(show_spinner="Loading Data")
def load_data(file_path):
    # This line just resets the session state if a new data file is selected
    st.session_state.v1index, st.session_state.v2index = None, None
    # Grab data
    df = pd_read_file(file_path)
    # Make columns lower for consistency (if they're strings)
    try:
        df.columns = df.columns.str.lower()
    except:
        pass

    return df


@st.cache_data(show_spinner="Loading Descriptions")
def load_desc(file_path):
    # Grab descriptions
    df_desc = pd_read_file(file_path)
    try:
        df_desc.iloc[:, 0] = df_desc.iloc[:, 0].str.lower()
    except:
        pass

    # Add a suffix to duplicate descriptions
    def append_dupe_labels(df):
        seen_names = {}
        modified_df = df.copy()
        for i, name in enumerate(modified_df.iloc[:, 1]):
            if name in seen_names:
                seen_names[name] += 1
                modified_df.at[i, 1] = f"{name} {seen_names[name]}"
            else:
                seen_names[name] = 1
        df.iloc[:, 1] = modified_df.iloc[:, 1]
        return df
    
    df_desc = append_dupe_labels(df_desc)

    return df_desc

@st.cache_data(show_spinner="Loading Encodings")
def load_enc(file_path):

    # Grab encodings
    df_enc = pd_read_file(file_path)
    try:
        df_enc.iloc[:, 0] = df_enc.iloc[:, 0].str.lower()
    except:
        pass

    return df_enc

def main():

    # Initialize dataframes for data, descriptions, and encodings.
    df, df_desc, df_enc = None, None, None
    descfile, encfile = None, None

    # Add sidebar for loading data
    with st.sidebar:
        st.title("Load Data")
        datatab, desctab, enctab = st.tabs(["Data", "Descriptions", "Encodings"])
        with datatab:
            datafile = st_file_browser(key = 'A', path=data_lake_location, show_preview=False)
        with desctab:
            if st.checkbox( "Descriptions file (optional)", key="desccheck", help="File with descriptions of variables, first column should be variable name, second column should be description. Information in additional columns (>2) will be displayed below the graph."):
                descfile = st_file_browser(key = 'B', path=data_lake_location, show_preview=False)
        with enctab:
            if st.checkbox("Encodings file (optional)", help="File with encodings of variables, first column should be variable name, second column should be dict of encodings."):
                encfile = st_file_browser(key = 'C', path=data_lake_location, show_preview=False)

    # Fill dataframes if files are selected
    if datafile:
        df = load_data(datafile['target']['path'])
        labels = df.columns.tolist()
        with st.sidebar:
            with datatab:
                st.subheader("Data Preview")
                st.dataframe(df.head(5))
    if descfile:
        df_desc = load_desc(descfile['target']['path'])
        # Use descriptions as labels, if available
        replacement_dict = dict(zip(df_desc.iloc[:, 0], df_desc.iloc[:, 1]))
        for label in labels:
            if label in replacement_dict:
                labels[labels.index(label)] = replacement_dict[label]
        with st.sidebar:
            with desctab:
                st.subheader("Preview")
                st.dataframe(df_desc.head(5))
    if encfile:
        df_enc = load_enc(encfile['target']['path'])
        with st.sidebar:
            with enctab:
                st.subheader("Preview")
                st.dataframe(df_enc.head(5))

    # Function to grab and process an input variable
    def grab_and_process(v):
        vlabel = v
        if descfile:
            if v in df_desc.iloc[:, 1].tolist():
                v = df_desc.iloc[:, 0][df_desc.iloc[:, 1] == v].tolist()[0]
        vdata, enc_count = apply_encoding(v)
        vdata = vdata.dropna()
        return v, vlabel, vdata, enc_count
    
    # Function to apply encodings, if available
    def apply_encoding(var):
        def to_int(s):
            try:
                return int(s)
            except:
                return s
        # Check for encoding
        try:
            encoding = df_enc.iloc[:, 1][df_enc.iloc[:, 0] == var].to_list()[0]
            # If all values are categorical, return mapped values
            # Use dictionary comprehension to filter out keys with None values and convert keys to int
            encoding = {to_int(key): value for key, value in encoding.items() if value is not None}   
            not_encoded = df[var][~df[var].isin(encoding.keys())]
            if not_encoded.nunique() == 0:
                # sort values so they get graphed properly
                sorted_df = df.sort_values(by=var)
                return sorted_df[var].replace(encoding), {}
            # Otherwise, seperate numerical values from categorical (e.g., 0-79 from (80:80 years and over))
            enc_count = {}
            for k, v in encoding.items():
                try:
                    s = (df[var] == k).sum()
                    if s > 0:
                        enc_count[v] = s
                except:
                    pass
            return not_encoded, enc_count
        except:
            return df[var], {}
  
    # Generate title
    st.title("Data Visualizer")
    # Show empty graph on startup for aesthetics
    show_empty = True

    if datafile:
        st.markdown("<h1 style='font-size: 25px; color: grey;'>Select variable(s)</h1>", unsafe_allow_html=True)
        #prompt = st.text_input(label="prompt", value=None, placeholder="Ask anything (e.g., does weight increase with age?)", help="Uses AI to find variables", max_chars=50, label_visibility="hidden")
        rand, col1, col2, col3, clear = st.columns([1, 7.5, 1, 7.5, 1])
        with rand:
            # Markdown for vertical alignment
            st.markdown("<div style='width: 1px; height: 28px'></div>", unsafe_allow_html=True)
            if st.button("🎲", help="Randomize"):
                r1 = np.random.randint(0, len(labels))
                r2 = np.random.randint(0, len(labels))
                st.session_state.v1index = r1
                st.session_state.v2index = r2
                st.rerun()
        with col1:
            v1 = st.selectbox('Variable 1',labels, index=st.session_state.v1index)
        
        #if prompt:
        #    answer = answer_question(prompt)
        #    v1 = df_desc.loc[answer[0], "Label"]

        if v1:
            show_empty = not show_empty
            v1, v1label, v1data, enc_count1 = grab_and_process(v1)
            with col3:
                v2 = st.selectbox('Variable 2',labels, index=st.session_state.v2index)
            
            #if len(answer) > 1:
            #    v2 = df_desc.loc[answer[1], "Label"]
            if not v2:
                # Create bins if data is numerical
                if np.issubdtype(v1data.dtype, np.number):
                    nbins = optml_nbins(v1data)
                    fig1 = px.histogram(v1data, nbins=nbins, text_auto=True)
                    if len(enc_count1) > 0:
                        fig2 = px.bar(x=enc_count1.keys(), y=enc_count1.values(), text_auto=True)
                        fig = make_subplots(rows=1, cols=2, subplot_titles=['Primary', 'Other'], column_widths=[nbins, 1], shared_yaxes=True)
                        fig.add_trace(fig1['data'][0], row=1, col=1)
                        fig.add_trace(fig2['data'][0], row=1, col=2)
                    else:
                        fig = fig1
                    avg = "%.2f" % np.mean(v1data)
                    std = "%.2f" % np.std(v1data)
                    fig.update_layout(xaxis_title=v1label, title_text=f'Distribution of {v1label} <br> N = {v1data.count()}, Average = {avg}, Standard Deviation = {std}')
                # Otherwise, just plot categorical data
                else:
                    fig = px.histogram(v1data, title=f'Distribution of {v1label} <br> N = {v1data.count()}',)
                    fig.update_layout(xaxis_title=v1label)
                
                pieoption, _ = st.columns([1, 6])
                # Pie chart option
                with pieoption:
                    if st.button("Pie Chart"):
                        fig = px.pie(v1data, names = v1data, title=f'Distribution of {v1label}')
                        fig.update_traces(textinfo='value+percent')
                

                # Display figure
                st.plotly_chart(fig, use_container_width=True)
                # Add details, if available
                if descfile:
                    if len(df_desc.columns) > 2:
                        if v1 in df_desc.iloc[:, 0].tolist():
                            st.subheader(v1label, divider=True)
                            for i in range(2, len(df_desc.columns)):
                                details = df_desc.iloc[:, i][df_desc.iloc[:, 1] == v1label].tolist()[0]
                                if details != v1label:
                                    st.write(str(df_desc.columns[i]) + ": ", details)
            
            if v2:
                v2, v2label, v2data, enc_count2 = grab_and_process(v2)
                if st.session_state.v2index != None:
                    with clear:
                        # Markdown for vertical alignment
                        st.markdown("<div style='width: 1px; height: 28px'></div>", unsafe_allow_html=True)
                        if st.button("$\otimes$", help="Clear 2nd Variable"):
                            st.session_state.v2index = None
                            st.rerun()

                # 4 cases generated via: v1, v2 in {categorical, numerical}
                case = check_case(v1data, v2data)
                # combine v1 and v2 (only keeping rows where both values are non-empty)
                df_combined = join_data(v1data, v2data, case)


                if len(df_combined) == 0:
                    # Create row for buttons (only for aeshetics in this case)
                    but = st.columns(1)
                    show_empty = True
                    if len(df.columns == 1):
                        st.markdown("<h1 style='text-align: center; font-size: 25px; color: grey;'>Sadly, these variables are in an incompatible format. Please select different ones.</h1>", unsafe_allow_html=True)
                    else:
                        st.markdown("<h1 style='text-align: center; font-size: 25px; color: grey;'>Sadly, these two variables do not share any non-zero rows.</h1>", unsafe_allow_html=True)
                else:
                    v1data = df_combined[v1]
                    v2data = df_combined[v2]
                    with col2:
                        # Markdown for vertical alignment
                        st.markdown("<div style='width: 1px; height: 28px'></div>", unsafe_allow_html=True)
                        swap = st.button("$\leftrightarrow$", help="Swap Axes")

                    def case1(data1, data2, label1, label2):
                        corr, p = grab_stats(1, data1, data2)
                        # Create a Plotly scatterplot
                        fig = px.scatter(x=data1, y=data2, labels={'x': label1, 'y': label2}, title=f'{label2} vs {label1} <br> Correlation = {corr}, p {p}')
                        return fig

                    # Case 1: Both numerical
                    if case == 1:
                        # Create row for buttons (only for aeshetics in this case)
                        but = st.columns(1)
                        fig = case1(v1data, v2data, v1label, v2label)


                    # Case 2: v1 numerical, v2 categorical
                    # Case 3: v1 categorical, v2 numerical
                    elif case in (2,3):
                        # Option A: bar graph with average of v1 for each cat in v2
                        def plot_bar_chart(df, v1, v2, v2label, v1label, f, p):
                            cats = df.groupby(v2, sort=False).mean().index.tolist()
                            avgs = df.groupby(v2, sort=False).mean()[v1].values
                            if case == 2:
                                return px.bar(x=cats, y=avgs, labels={'x': v2label, 'y': v1label}, title=f'Average {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}', text_auto='.2f')
                            if case == 3:
                                return px.bar(x=avgs, y=cats, labels={'x': v1label, 'y': v2label}, title=f'Average {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}', text_auto='.2f')
                        # Option B: histogram of distribution of v1 for each cat in v2
                        def plot_histogram(df, v1, v2, v2label, v1label, f, p):
                            if case == 2:
                                fig = px.histogram(df, x=v1, color=v2, title=f'Distribution of {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}', 
                                                barmode='overlay', opacity=0.4)
                                fig.update_layout(xaxis_title_text=v1label, yaxis_title_text=f"Count of {v2label}")
                            if case == 3:
                                fig = px.histogram(df, y=v1, color=v2, title=f'Distribution of {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}', 
                                                barmode='overlay', opacity=0.4)
                                fig.update_layout(yaxis_title_text=v1label, xaxis_title_text=f"Count of {v2label}")
                            return fig

                        # Option C: box plot of v1 for each cat in v2
                        def plot_box_plot(v2data, v1data, v2label, v1label, f, p):
                            if case == 2:
                                return px.box(x=v2data, y=v1data, labels={'x': v2label, 'y': v1label}, title=f'Distribution of {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}')
                            if case == 3:
                                return px.box(y=v2data, x=v1data, labels={'x': v1label, 'y': v2label}, title=f'Distribution of {v1label} by {v2label} <br> N = {v1data.count()}, F = {f}, p {p}')

                        # Create columns for buttons
                        but1, but2, but3, _ = st.columns([1, 1, 1, 6])

                        def cases_2and3(df, v1, v2, v1data, v2data, v2label, v1label):
                            # Get stat
                            f_stat, p = grab_stats(case, v1data, v2data)
                            # Default to option A
                            fig = plot_bar_chart(df, v1, v2, v2label, v1label, f_stat, p)
                            if but1.button('Bar Chart'):
                                fig = plot_bar_chart(df, v1, v2, v2label, v1label, f_stat, p)
                            # Allow option B iff 2 variables in categorical data
                            if v2data.nunique() == 2:
                                if but3.button("Histogram"):
                                    fig = plot_histogram(df, v1, v2, v2label, v1label, f_stat, p)
                            # In any case, display box plot option (option C)
                            if but2.button('Box Plot'):
                                fig = plot_box_plot(v2data, v1data, v2label, v1label, f_stat, p)
                            return fig
                        
                        if case == 2:
                            fig = cases_2and3(df_combined, v1, v2, v1data, v2data, v2label, v1label)
                        if case == 3:
                            fig = cases_2and3(df_combined, v2, v1, v2data, v1data, v1label, v2label)

                    # Case 4: Both categorical
                    elif case == 4:
                        but1, but2, _ = st.columns([1, 1, 4])
                        chi2, p = grab_stats(case, v1data, v2data)
                        # Option 4A: Clustered Bar Chart
                        fig = px.histogram(df_combined, x=v1, color=v2, barmode='group', text_auto=True)
                        # Option 4B: Stacked Bar Chart
                        if but1.button("Clustered Bar Chart"):
                            fig = px.histogram(df_combined, x=v1, color=v2, barmode='group', text_auto=True)
                        if but2.button("Stacked Bar Chart"):
                            try:
                                grouped_df = df_combined.groupby([v1, v2]).size().reset_index(name='Count')
                            except:
                                grouped_df = df_combined.groupby([v1]).size().reset_index(name='Count')
                            fig = px.bar(grouped_df, x=v1, y="Count", color=v2, text = "Count")
                        fig.update_layout(xaxis_title_text=v1label, yaxis_title_text=f'Count of {v2label}',
                            title=f'Distribution of {v2label} by {v1label} <br> N = {v2data.count()}, chi-squared = {chi2}, p {p}')
                    

                    # Display the Plotly figure in Streamlit
                    st.plotly_chart(fig, use_container_width=True)

                    # Display details, if available
                    if descfile and len(df_desc.columns) > 2:
                        for v in [v1, v2]:
                            if v in df_desc.iloc[:, 0].tolist():
                                label = df_desc.iloc[:, 1][df_desc.iloc[:, 0] == v].tolist()[0]
                                st.subheader(label, divider=True)
                                for i in range(2, len(df_desc.columns)):
                                    details = df_desc.iloc[:, i][df_desc.iloc[:, 1] == label].tolist()[0]
                                    if details != label:
                                        st.write(str(df_desc.columns[i]) + ": ", details)

                    if swap and (v1 != v2):
                        st.session_state.v1index = list(labels).index(v2label)
                        st.session_state.v2index = list(labels).index(v1label)
                        st.rerun()
            
    # Show empty graph on startup for aesthetics        
    if show_empty:
        empty_fig = go.Figure()
        empty_fig.update_layout(xaxis=dict(showticklabels=False), yaxis=dict(showticklabels=False))
        st.plotly_chart(empty_fig, use_container_width=True)

# Calculate optimal number of bins for histogram
def optml_nbins(data):
    iqr = np.percentile(data, 75) - np.percentile(data, 25)
    if iqr > 0:
        h = 2 * iqr / (len(data) ** (1/3))
        optimal_nbins = int((np.max(data) - np.min(data)) / h)
        return optimal_nbins
    else:
        return 1

def join_data(data1, data2, case):
    df1, df2 = data1.to_frame(), data2.to_frame()
    try:
        if case in (1,2):
            df_join = df2.join(df1, how='inner')
        elif case == 3:
            df_join = df1.join(df2, how='inner')
        elif case == 4:
            # Sort data on ordered variable (e.g., time spent in US should order, but Gender should not)
            digits1 = any(any(char.isdigit() for char in value) for value in data1.iloc[:5])
            digits2 = any(any(char.isdigit() for char in value) for value in data2.iloc[:5])
            if digits2 and not digits1:
                df_join = df2.join(df1, how='inner')
            else:
                df_join = df1.join(df2, how='inner')
        else:
            return pd.DataFrame(columns=["Error"])
    except ValueError:
            df_join = df1
    return df_join

    
# What about case where data1 or data2 is mixed? Like age
def check_case(data1, data2):
    if np.issubdtype(data1.dtype, np.number) and np.issubdtype(data2.dtype, np.number):
        return 1
    if np.issubdtype(data1.dtype, np.number) and pd.api.types.is_string_dtype(data2):
        return 2
    if pd.api.types.is_string_dtype(data1) and np.issubdtype(data2.dtype, np.number):
        return 3
    if pd.api.types.is_string_dtype(data1) and pd.api.types.is_string_dtype(data2):
        return 4
    
def grab_stats(case, data1, data2):
    def plabel(p):
        if p >= .001:
            return "= " + '%.3f' % p
        else:
            return "< .001"
    if case == 1:
        # Find Pearson correlation
        corr, p = scipy.stats.pearsonr(data1, data2)
        corr = '%.2f' % corr
        p = plabel(p)
        return corr, p
    if case in (2,3):
        # Perform one-way ANOVA
        groups = {group: [] for group in set(data2)}
        if len(groups) > 1:
            for value, group in zip(data1, data2):
                groups[group].append(value)
            f_stat, p = scipy.stats.f_oneway(*groups.values())
            f_stat = '%.2f' % f_stat
            p = plabel(p)
            return f_stat, p
        return "n/a", "n/a"
    if case == 4:
        # Perform Chi-squared analysis
        contingency_table = pd.crosstab(data1, data2)
        if len(contingency_table) > 1:
            chi2, p, dof, expected = scipy.stats.chi2_contingency(contingency_table)
            chi2 = '%.2f' % chi2
            p = plabel(p)
            return chi2, p
        return "n/a", "= n/a"

        



if __name__ == "__main__":
    main()