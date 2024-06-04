import streamlit as st
import graphviz as graphviz
from constants import *
from neo4j_controller import Neo4jController
import pandas as pd
import matplotlib.pyplot as plt


# Display app title
st.title("NYPD Arrest Data")

# Create a Neo4jRepository object
n4j = Neo4jController(
    st.secrets['neo4j_uri'], 
    st.secrets['neo4j_user'], 
    st.secrets['neo4j_password']) 



def page_home():
    meetups = n4j.get_arrests()
    meetups_df = pd.DataFrame(meetups, columns=["Arrest Date", "Arrests Count"])
    meetups_df.index = meetups_df.index + 1
    # Display the DataFrame as a table
    st.table(meetups_df)
    # Convert neo4j.time.Date to datetime
    meetups_df["Arrest Date"] = meetups_df["Arrest Date"].apply(lambda x: x.to_native())
        # Convert "Arrest Date" to datetime
    meetups_df["Arrest Date"] = pd.to_datetime(meetups_df["Arrest Date"])

    # Extract month from "Arrest Date"
    meetups_df["Month"] = meetups_df["Arrest Date"].dt.month

    # Group by month and count arrests
    monthly_arrests = meetups_df.groupby("Month")["Arrests Count"].sum()

    # Plot the data
    plt.figure(figsize=(10, 6))
    monthly_arrests.plot(kind="bar", color="skyblue")
    plt.title("Number of Arrests by Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Arrests")
    plt.xticks(rotation=0)  # Rotate x-axis labels if needed
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()

    # Display the plot in Streamlit
    st.pyplot(plt)

def page_analyics():
    st.title("Analytics")
    st.subheader("Hotspots")
    hotspots= n4j.get_hotspots()
    hotspots_df = pd.DataFrame(hotspots, columns=["Borough","Precinct", "Arrests Count"])
    hotspots_df.index = hotspots_df.index + 1
    # Display the DataFrame as a table
    st.table(hotspots_df)
    borough_arrests = hotspots_df.groupby("Borough")["Arrests Count"].sum().reset_index()
    
    st.subheader("Hotspots Bar Chart")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(borough_arrests['Borough'], borough_arrests['Arrests Count'])
    ax.set_xlabel('Borough')
    ax.set_ylabel('Arrests Count')
    ax.set_title('Hotspots')
    st.pyplot(fig)
    ages = n4j.age_cat()
    ages_df = pd.DataFrame(ages, columns=["Age Category", "Arrests Count"])
    ages_df.index = ages_df.index + 1
    st.table(ages_df)
    st.subheader("Age Category Pie Chart")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(ages_df['Arrests Count'], labels=ages_df['Age Category'], autopct='%1.1f%%')
    ax.set_title('Age Category')
    st.pyplot(fig)
    
    st.subheader("Most Common Crimes")
    most_common_crimes = n4j.common_crimes()
    most_common_crimes_df = pd.DataFrame(most_common_crimes, columns=["Most Common Crimes","Count"])
    most_common_crimes_df.index = most_common_crimes_df.index + 1
    # Display the DataFrame as a table
    st.table(most_common_crimes_df)

    st.subheader("Arrests By Race")
    arrests= n4j.arrests_by_race()
    race_df = pd.DataFrame(arrests, columns=["Race", "Arrest Count"])
    race_df.index = race_df.index + 1
    st.table(race_df)
    st.subheader("Arrests By Race Pie Chart")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(race_df['Arrest Count'], labels=race_df['Race'], autopct='%1.1f%%')
    st.pyplot(fig)

    st.subheader("Arrests By Gender")
    arrestss= n4j.arrests_by_gender()
    print(arrestss,"ddd")
    gen_df = pd.DataFrame(arrestss, columns=["Gender", "Arrest Count"])
    gen_df.index = gen_df.index + 1
    st.table(gen_df)
    st.subheader("Arrests By Gender Pie Chart")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(gen_df['Arrest Count'], labels=gen_df['Gender'], autopct='%1.1f%%')
    st.pyplot(fig)

    
    st.title("GDS Library")
    st.subheader("Community Sizes")
    communities = n4j.community()
    communities_df = pd.DataFrame(communities, columns=["Community", "Community Size"])
    communities_df.index = communities_df.index + 1
    # Display the DataFrame as a table
    st.table(communities_df) 
    st.subheader("Precints with the most arrests Degree Centrality")
    degree_centrality = n4j.degree_centrality()
    degree_centrality_df = pd.DataFrame(degree_centrality, columns=["Precinct", "Score"])
    degree_centrality_df.index = degree_centrality_df.index + 1
    # Display the DataFrame as a table
    st.table(degree_centrality_df)



    

def page_stats():
    st.title("Graph Statistics")
    numNodes = n4j.total_nodes()
    st.write("Total number of Nodes:", numNodes[0]['total_nodes'])
    numrs = n4j.num_relationships()
    st.write("Total number of relationships:", numrs[0]['total_relationships'])
    isolatedNodes = n4j.isolated_nodes()
    st.write("Number of Isolated Nodes:", isolatedNodes[0]['isolated_nodes'])
    crimes = n4j.crimes_committed()
    st.write("Number of Crimes Committed:", crimes[0]['crime_relationships'])
    num_crimes = n4j.num_crimes()
    st.write("Number of Distinct Crimes:", num_crimes[0]['UniqueOffenseCount'])
    crimeList = n4j.crime_list()
    
    crimeList_df = pd.DataFrame(crimeList, columns=["Crime"])
    crimeList_df.index = crimeList_df.index + 1
    # Display the DataFrame as a table
    st.table(crimeList_df)

    

    


# Define the pages dictionary
pages = {
    "Arrests Overview": page_home,
    "Hotspots": page_analyics,
    "Graph Statistics": page_stats
}

# Streamlit App
def main():
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(pages.keys()))

    # Run the function associated with the selected page
    pages[selection]()

if __name__ == "__main__":
    main()

