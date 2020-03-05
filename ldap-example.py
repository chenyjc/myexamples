from ldap3 import Server, Connection, AUTO_BIND_NO_TLS, SUBTREE, ALL_ATTRIBUTES, ALL
import json
# import smtplib
# from email.mime.text import MIMEText

def get_ldap_info(email):
    with Connection(Server('the-ldap-server', port=636, use_ssl=True),
        auto_bind=AUTO_BIND_NO_TLS,
        read_only=True,
        check_names=True) as c:
        c.search(search_base='ou=People,o=hp.com', search_filter='(mail={0})'.format(email), attributes=['Status','manager','OrganizationChart','OrganizationChartAcronym','JobFamily'])#         c.search(search_base='ou=People,o=hp.com', search_filter='(mail={0})'.format(email), attributes=ALL_ATTRIBUTES)
    #print(c.response_to_json())
    #print(c.result)
    return c.response_to_json()
    
    
import pandas as pd
import logging

def parse(email,ldap_info):
    try:
        content = json.loads(ldap_info)
        Status=''
        JobFamily=''
        OrganizationChartAcronym=''
        OrganizationChart=''
        manager=''
        if len(content['entries'])>0:
            if len(content['entries'][0]["attributes"]["Status"]) >0 :
                Status=content['entries'][0]["attributes"]["Status"][0] 
            if len(content['entries'][0]["attributes"]["JobFamily"]) >0 :
                JobFamily=content['entries'][0]["attributes"]["JobFamily"][0]
            if len(content['entries'][0]["attributes"]["OrganizationChartAcronym"]) >0 :
                OrganizationChartAcronym=content['entries'][0]["attributes"]["OrganizationChartAcronym"][0]
            if len(content['entries'][0]["attributes"]["OrganizationChart"]) >0 :
                OrganizationChart=content['entries'][0]["attributes"]["OrganizationChart"][0]
            if len(content['entries'][0]["attributes"]["manager"]) >0 :
                manager=content['entries'][0]["attributes"]["manager"][0]
                manager=manager.split(',')[0].split('=')[1]
        return [email,Status,manager,JobFamily,OrganizationChartAcronym,OrganizationChart]
    except Exception as e:
        logging.exception(e)
        return []
        
        
users=['test@test.com']

columns=['email','Status','manager','JobFamily','OrganizationChartAcronym','OrganizationChart']

data=[]

for email in users:
    print(email)
    data.append(parse(email,get_ldap_info(email)))


df = pd.DataFrame(data,columns=columns)
df.to_excel('UserInfo.xlsx')        
