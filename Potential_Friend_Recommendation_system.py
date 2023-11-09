# %%
!pip3 install neo4j


# %% [markdown]
# ## Importing data into the Environment

# %% [markdown]
# ### Data is in CSV format so we used csv module and then also since the way the data was structures each line of CSV has user data and followed by a list of all the ID's of the friends hence using regular expressin to separate the user data and the friends list

# %%
import csv
import re
pattern = r'"(.*?)"'
user_data=[]
friends_list=[]
with open('/Users/admin/Downloads/data 2.csv', mode='r') as file:
    csv_reader = csv.reader(file)
    check=0
    for row in csv_reader:
        # Access data in each row using row[index]
        user_data.append(row[:9])
        friends_list.append(re.findall(pattern, "".join(row[9:])))



# %% [markdown]
# ## Data Cleaning

# %% [markdown]
# ### After loading the data into the environment we noticed there were some nodes with special characters and so we had to remove those characters

# %%
for i in user_data:
    i[0]=re.sub(r"'", '', i[0])

# %%
for i in user_data:
    print(i)

user_data.remove(['id', 'screenName', 'tags', 'avatar', 'followersCount', 'friendsCount', 'lang', 'lastSeen', 'tweetId']
)
user_data

# %%
friends_list.remove([])
friends_list

# %% [markdown]
# # Database Connection

# %%
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"  # Replace with your Neo4j database URL
user = "neo4j"         # Replace with your Neo4j username
password = "12345678"     # Replace with your Neo4j password

driver = GraphDatabase.driver(uri, auth=(user, password))


# %% [markdown]
# ## To create Nodes in the Database

# %% [markdown]
# ### Here we can use create command but using create we can only add new nodes and cannot update already existing nodes so we use Merge command to create if the node is not in the database else update already existing node

# %%
def merge_and_update(tx, id ,name=None, tags=None, followers=None, friends=None):
    query = (
        "MERGE (n:Person {Id: $id}) "
        "ON CREATE SET n.UserName = $name, n.Tags = $tags, n.Followers = $followers, n.Friends = $friends "
        "ON MATCH SET n.UserName = $name, n.Tags = $tags, n.Followers = $followers, n.Friends = $friends"
    )
    tx.run(query, id=id,name=name, tags=tags, followers=followers, friends=friends)

# %%
def create_person_node(tx, id ,name=None, tags=None, followers=None, friends=None):
    query = (
        "CREATE (p:Person {Id:$id,UserName: $name, Tags:$tags,Followers:$followers,Friends:$friends})"
        "RETURN p"
        "MERGE (n:Person {Id:$id,UserName: $name, Tags:$tags,Followers:$followers,Friends:$friends})"
        "SET n.id = $id, n.UserName = $name, n.Tags = $tags, n.Followers = $followers, n.Friends = $friends"

    )
    result = tx.run(query, id=id, name=name, tags=tags, followers=followers, friends=friends)
    return result.single()[0]
driver.close()


# %% [markdown]
# ## Create Relation Function

# %% [markdown]
# ### Here we are creating a relationship between 2 nodes 

# %%
def create_relation_node(tx, id1, id2):
    query = '''
    MATCH (p:Person {Id: $person_id}), (f:Person {Id: $friend_id})
    WHERE NOT (p)-[:Friends_With]->(f)
    CREATE (p)-[:Friends_With]->(f)
    '''
    parameters = {'person_id': id2, 'friend_id': id1}
    tx.run(query, parameters)


# %% [markdown]
# ## Loading Data into GraphDB using Create Relation and merge node function

# %% [markdown]
# ### While loading the data into the database it was a tedious task as the data was very large and since each node or a person atleast had 100 friends on an average so looping through all the users and creating the nodes for them took a lot of time

# %% [markdown]
# ### Below the way we are creating the nodes is first creating the user node then creating all the friend nodes for that user nodes and then adding relations between them. So in that case if we encounter that friend node afterwards we would have to just update the existing node in the data base

# %%
check=0
for i in range(1000):
    with driver.session() as session:
        session.write_transaction(merge_and_update, user_data[i][0], user_data[i][1], user_data[i][2], user_data[i][4], user_data[i][5])
    for j in friends_list[i]:
        with driver.session() as session:
            session.write_transaction(merge_and_update, j, None, None, None, None)
            session.write_transaction(create_relation_node,user_data[i][0],j)
    driver.close()
    

    

# %% [markdown]
# ## Function which contains the query to find the Potential friend basing on the number of mutual friends both the persons have. Here we checkfor n:person-relation->mutual:person<-relation-p:person and then count(mutual) then give the reult in a descending order of the number of mutuals. Higher the number of mutuals between 2 nodes then there is a higher chance of the both the nodes becoming friends

# %%
import re

# %%
def potential_friends(tx,id):
    query=(
        "MATCH (n:Person {Id:$id})-[r:Friends_With]->(person)<-[:Friends_With]-(q:Person) "
        "WHERE n<>q and NOT  (n)-[:Friends_With]->(q) "
        "return q, count(person) as mutuals "
        "Order by mutuals DESC "
        "Limit 5"
    )
    result=tx.run(query,id=id)
    print(f"id        mutuals" )
    for record in result:
        #print(record)
        node = record['q']
        mutuals = record['mutuals']
        
        if node:
            match = re.search(r"'Id': '(\d+)'", str(node))
            id_value = match.group(1)
            print(f"{ id_value}     {mutuals}" )

        else:
            print("Id property not found in the record.")
    driver.close()
    return result



# %% [markdown]
# ## Here we are taking user ID on which we have to compute the query give the user's potential friends basing on the mutual friends

# %%
def User_friend_recommendation():
    userid=input("Enter the ID : ")
    with driver.session() as session:
        session.write_transaction(potential_friends,userid)
        driver.close()


# %%
User_friend_recommendation()

# %%



