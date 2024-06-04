from neo4j import GraphDatabase, unit_of_work

# Static units of work that can be retried if they fail

@unit_of_work(timeout=5)
def _get_arrests(tx):
    result = tx.run("""
        MATCH (p:Person)-[r:ARRESTED_AT]->(a:Address)
        RETURN date(r.arrest_date) AS arrest_date, count(p) AS arrests_count
        ORDER BY arrest_date
    """)
    # return [(record["arrest_date"], record["arrests_count"]) for record in result.data()]
    return [(record["arrest_date"], record["arrests_count"]) for record in result.data()]

@unit_of_work(timeout=5)
def _get_hotspots(tx):
    result = tx.run("""
        MATCH (l:Location)<-[:ARRESTED_AT]-(p:Person)
        RETURN l.arrest_boro AS borough, l.arrest_precinct AS precinct, count(p) AS num_arrests
        ORDER BY num_arrests DESC

    """)
    return [(record["borough"], record["precinct"], record["num_arrests"]) for record in result.data()]

@unit_of_work(timeout=5)
def _age_cat(tx):
    result = tx.run("""
        MATCH (p:Person)-[:BELONGS_TO]->(a:AgeGroup)
    WITH a, count(p) AS arrest_count
    RETURN a.age_group AS age, arrest_count
    ORDER BY arrest_count DESC
    """)
    return [(record["age"], record["arrest_count"]) for record in result.data()]

@unit_of_work(timeout=5)
def _arrests_by_race(tx):
    result = tx.run("""
        MATCH (p:Person)
        RETURN p.perp_race AS Race,  COUNT(*) AS ArrestCount
        ORDER BY ArrestCount DESC

    """)
    return [(record["Race"], record["ArrestCount"]) for record in result.data()]

@unit_of_work(timeout=5)
def _arrests_by_gender(tx):
    result = tx.run("""
        MATCH (a:Person)
        RETURN a.perp_sex AS Gender, COUNT(*) AS ArrestCount

    """)
    return [(record["Gender"], record["ArrestCount"]) for record in result.data()]

@unit_of_work(timeout=5)
def _total_nodes(tx):
    result = tx.run("""
        MATCH (n) RETURN count(n) AS total_nodes;
    """)
    return result.data()

@unit_of_work(timeout=5)
def _isolated_nodes(tx):
    result = tx.run("""
        MATCH (n) WHERE NOT (n)--() RETURN count(n) AS isolated_nodes;

    """)
    return result.data()

@unit_of_work(timeout=5)
def _crimes_committed(tx):
    result = tx.run("""
        MATCH ()-[r:COMMITTED]->() RETURN count(r) AS crime_relationships;

    """)
    return result.data()

@unit_of_work(timeout=5)
def _num_crimes(tx):
    result = tx.run("""
        MATCH (n:Crime)
        RETURN COUNT(DISTINCT n.offense_desc) AS UniqueOffenseCount;
    """)
    return result.data()

@unit_of_work(timeout=5)
def _num_relationships(tx):
    result = tx.run("""
    MATCH ()-[r]->()
    RETURN count(r) AS total_relationships;

    """)
    return result.data()

@unit_of_work(timeout=5)
def _crime_list(tx):
    result = tx.run("""
        MATCH (n:Crime)
        RETURN DISTINCT n.offense_desc AS Crime

    """)
    return [(record["Crime"]) for record in result.data()]

@unit_of_work(timeout=5)
def _community(tx):
    result = tx.run("""
        CALL gds.louvain.stream('mygraph2')
        YIELD nodeId, communityId
        WITH communityId, COUNT(*) AS communityCount
        RETURN communityId AS crime_community, communityCount
        ORDER BY communityCount DESC
        LIMIT 20

    """)
    return [(record["crime_community"], record['communityCount']) for record in result.data()]

@unit_of_work(timeout=5)
def _degree_centrality(tx):
    result = tx.run("""
        CALL gds.degree.stream('mygraph')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId).arrest_precinct AS location, score
        RETURN location, score
        ORDER BY score DESC
        LIMIT 10;

    """)
    return [(record["location"], record['score']) for record in result.data()]

@unit_of_work(timeout=5)
def _common_crimes(tx):
    result = tx.run("""
        MATCH (p:Person)-[:COMMITTED]->(c:Crime)
        WITH c.offense_desc AS most_common_crime, count(*) AS crime_count
        RETURN most_common_crime, crime_count
        ORDER BY crime_count DESC
        LIMIT 5

    """)
    
    return [(record["most_common_crime"],record["crime_count"]) for record in result.data()]





class Neo4jController:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def get_arrests(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_get_arrests)
        except Exception as e:
            print("get_arrests failed:", e)

    def get_hotspots(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_get_hotspots)
        except Exception as e:
            print("get_hotspots failed:", e)

    def age_cat(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_age_cat)
        except Exception as e:
            print("age_cat failed:", e)

    def arrests_by_gender(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_arrests_by_gender)
        except Exception as e:
            print("age_cat failed:", e)        

    def arrests_by_race(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_arrests_by_race)
        except Exception as e:
            print("age_cat failed:", e)       

    def total_nodes(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_total_nodes)
        except Exception as e:
            print("total_nodes failed:", e)

    def isolated_nodes(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_isolated_nodes)
        except Exception as e:
            print("total_nodes failed:", e)  

    def crimes_committed(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_crimes_committed)
        except Exception as e:
            print("total_nodes failed:", e) 

    def crime_list(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_crime_list)
        except Exception as e:
            print("total_nodes failed:", e) 

    def num_crimes(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_num_crimes)
        except Exception as e:
            print("total_nodes failed:", e)  

    def num_relationships(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_num_relationships)
        except Exception as e:
            print("total_nodes failed:", e)            

    def community(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_community)
        except Exception as e:
            print("get_arrests failed:", e)                                

    def degree_centrality(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_degree_centrality)
        except Exception as e:
            print("get_graph_data failed:", e)

    def common_crimes(self):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                return session.execute_read(_common_crimes)
        except Exception as e:
            print("get_graph_data failed:", e)        

