import psycopg2
import json
# numpy used only for intersected arrays
import numpy as np

# Open and load json file
f = open('configClear_v2.json')
data = json.load(f)

# Groups to be intersected, order matters, Port-channel has to be before Ethernet
wanted_list = np.array(['Port-channel','TenGigabitEthernet','GigabitEthernet'])
# List of all Groups
all_list =  np.array([])
# Root path of nested json
root = data['frinx-uniconfig-topology:configuration']['Cisco-IOS-XE-native:native']['interface']

for i in root:
    all_list = np.append(all_list,i,axis=None)

# Intersected groups used for loop
intersected = np.intersect1d(wanted_list,all_list)

# Table fields, order matters, port_channel_id has to be last
# name, config, port_channel_id are connected with if statements
fields_to_fill = ['name','description','mtu','config','port_channel_id']


# PostgreSQL operations
con = None
cursor = None
return_id = False
# Name,Id; it's easier that way to get the id for port-channel
portchannels = {}
sql_query = '''INSERT INTO interfaces(name, description, max_frame_size, config,
   port_channel_id) VALUES ('''
# Sending data
try:
    con = psycopg2.connect("host='localhost' dbname='base' user='postgres' password='password123'")
    cursor = con.cursor()
    for group in intersected:
        # for item in group
        for i in range(0,len(root[group])):
            # for field to insert
            for field in fields_to_fill:
                # check config
                if field == 'config':
                    config = root[group][i]
                    sql_query += "'" + str(json.dumps(config)) + "', "
                # check name
                elif field == 'name':
                    sql_query += "'" + str(group) + str(root[group][i][field]) + "', "
                    if group == 'Port-channel':
                        return_id = True
                # port_channel_id flow is following:
                # first looped group should be Port-channel
                # id of submited Port-channel item is stored
                # inside of portchannels dict 
                # if the item of current group has setting:
                # Cisco-IOS-XE-ethernet:channel-group
                # then get value from portchannels dict
                elif field == 'port_channel_id':
                    try:
                        channel_num = root[group][i]['Cisco-IOS-XE-ethernet:channel-group']['number']
                        if channel_num and channel_num != "None":
                            for portchannel in root['Port-channel']:
                                if portchannel['name'] == channel_num:
                                    if portchannels.get(str(channel_num)) != None:
                                        sql_query += f"{portchannels.get(str(channel_num))})"
                                    else:
                                        sql_query += 'null);'
                            
                    except:
                        # if somethings goes wrong it ends 
                        # with returning id or returning nothing
                        if return_id == True:
                            sql_query += 'null)'
                        else:
                            sql_query += 'null);'
                # other fields inside fields_to_fill
                else:
                    # if not none
                    try:
                        sql_query += " '" + str(root[group][i][field]) + "', "
                    # return null
                    except:
                        sql_query += 'null, '
            
            if return_id == True:
                sql_query += ' RETURNING id;'
            cursor.execute(sql_query)
            con.commit()
            if return_id == True:
                port_id = cursor.fetchone()[0]
                portchannels[str(root[group][i]["name"])] = port_id
            # Reset vars after item in group
            return_id = False
            sql_query = '''INSERT INTO interfaces(name, description, max_frame_size, config,
            port_channel_id) VALUES ('''


except psycopg2.DatabaseError as e:
    if con:
        con.rollback()
    
    print(f"Error, {e}")

finally:
    if con:
        cursor.close()
        con.close()