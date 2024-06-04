import graphviz as graphviz
import streamlit as st
# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = st.secrets['neo4j_uri']
AUTH = (st.secrets['neo4j_user'], st.secrets['neo4j_password'])

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

# Get the name of all 42 year-olds
records, summary, keys = driver.execute_query(
    "MATCH (p:Person)-[r:ARRESTED_AT]->(l:Location) RETURN date(r.arrest_date) AS arrest_date, count(p) AS arrests_count ORDER BY arrest_date",
    database_="neo4j",
)

# Loop through results and do something with them
for person in records:
    print(person)

# Summary information
print("The query `{query}` returned {records_count} records in {time} ms.".format(
    query=summary.query, records_count=len(records),
    time=summary.result_available_after,
))    